//! Embedding generation for semantic caching
//!
//! Provides abstractions for generating text embeddings from various sources.

use super::{EmbeddingModelConfig, EmbeddingModelType, SemanticError};
use std::sync::Arc;

/// Trait for embedding providers
pub trait EmbeddingProvider: Send + Sync {
    /// Generate an embedding for a single text
    fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError>;

    /// Generate embeddings for multiple texts (batch)
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        texts.iter().map(|t| self.embed(t)).collect()
    }

    /// Get the embedding dimension
    fn dimension(&self) -> usize;

    /// Get the model name/identifier
    fn model_name(&self) -> &str;
}

/// Embedding model that can generate embeddings
pub struct EmbeddingModel {
    provider: Arc<dyn EmbeddingProvider>,
}

impl EmbeddingModel {
    /// Create a new embedding model from configuration
    pub fn from_config(config: &EmbeddingModelConfig) -> Result<Self, SemanticError> {
        let provider: Arc<dyn EmbeddingProvider> = match config.model_type {
            EmbeddingModelType::None => Arc::new(NoOpProvider),
            EmbeddingModelType::Onnx => {
                // ONNX provider would require onnxruntime dependency
                // For now, return a placeholder
                Arc::new(PlaceholderProvider::new(
                    384,
                    "onnx-placeholder".to_string(),
                ))
            }
            EmbeddingModelType::OpenAI => Arc::new(OpenAIProvider::new(
                config.api_key.clone(),
                config.api_endpoint.clone(),
            )?),
            EmbeddingModelType::Cohere => Arc::new(CohereProvider::new(
                config.api_key.clone(),
                config.api_endpoint.clone(),
            )?),
            EmbeddingModelType::Custom => {
                let endpoint = config.api_endpoint.clone().ok_or_else(|| {
                    SemanticError::ConfigError("Custom endpoint requires api_endpoint".to_string())
                })?;
                Arc::new(CustomHttpProvider::new(endpoint, config.api_key.clone()))
            }
        };

        Ok(Self { provider })
    }

    /// Create a placeholder model (for testing or when auto-embed is disabled)
    pub fn placeholder(dimension: usize) -> Self {
        Self {
            provider: Arc::new(PlaceholderProvider::new(
                dimension,
                "placeholder".to_string(),
            )),
        }
    }

    /// Generate an embedding for text
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        self.provider.embed(text)
    }

    /// Generate embeddings for multiple texts
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        self.provider.embed_batch(texts)
    }

    /// Get the embedding dimension
    pub fn dimension(&self) -> usize {
        self.provider.dimension()
    }

    /// Get the model name
    pub fn model_name(&self) -> &str {
        self.provider.model_name()
    }
}

/// No-op provider that returns an error (for when auto-embed is disabled)
struct NoOpProvider;

impl EmbeddingProvider for NoOpProvider {
    fn embed(&self, _text: &str) -> Result<Vec<f32>, SemanticError> {
        Err(SemanticError::EmbeddingFailed(
            "Auto-embedding is disabled. Provide embeddings manually.".to_string(),
        ))
    }

    fn dimension(&self) -> usize {
        0
    }

    fn model_name(&self) -> &str {
        "none"
    }
}

/// Placeholder provider that generates deterministic embeddings based on text hash
/// Useful for testing and development
struct PlaceholderProvider {
    dimension: usize,
    name: String,
}

impl PlaceholderProvider {
    fn new(dimension: usize, name: String) -> Self {
        Self { dimension, name }
    }
}

impl EmbeddingProvider for PlaceholderProvider {
    fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Generate deterministic embedding based on text hash
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let hash = hasher.finish();

        // Use hash to seed a simple pseudo-random sequence
        let mut embedding = Vec::with_capacity(self.dimension);
        let mut state = hash;

        for _ in 0..self.dimension {
            // Simple LCG-like PRNG
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            // Normalize to [-1, 1]
            let value = ((state >> 32) as f32 / u32::MAX as f32) * 2.0 - 1.0;
            embedding.push(value);
        }

        // Normalize the vector
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut embedding {
                *x /= norm;
            }
        }

        Ok(embedding)
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        &self.name
    }
}

/// OpenAI embedding provider
struct OpenAIProvider {
    api_key: String,
    endpoint: String,
    model: String,
    dimension: usize,
    client: reqwest::blocking::Client,
}

/// OpenAI API request format
#[derive(serde::Serialize)]
struct OpenAIRequest {
    model: String,
    input: Vec<String>,
    encoding_format: String,
}

/// OpenAI API response format
#[derive(serde::Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbedding>,
}

#[derive(serde::Deserialize)]
struct OpenAIEmbedding {
    embedding: Vec<f32>,
}

/// OpenAI API error response
#[derive(serde::Deserialize)]
struct OpenAIErrorResponse {
    error: OpenAIErrorDetail,
}

#[derive(serde::Deserialize)]
struct OpenAIErrorDetail {
    message: String,
}

impl OpenAIProvider {
    const DEFAULT_ENDPOINT: &'static str = "https://api.openai.com/v1/embeddings";
    const DEFAULT_MODEL: &'static str = "text-embedding-3-small";

    fn new(api_key: Option<String>, endpoint: Option<String>) -> Result<Self, SemanticError> {
        let api_key = api_key
            .ok_or_else(|| SemanticError::ConfigError("OpenAI API key required".to_string()))?;

        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                SemanticError::ConfigError(format!("Failed to create HTTP client: {}", e))
            })?;

        Ok(Self {
            api_key,
            endpoint: endpoint.unwrap_or_else(|| Self::DEFAULT_ENDPOINT.to_string()),
            model: Self::DEFAULT_MODEL.to_string(),
            dimension: 1536,
            client,
        })
    }
}

impl EmbeddingProvider for OpenAIProvider {
    fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let request = OpenAIRequest {
            model: self.model.clone(),
            input: vec![text.to_string()],
            encoding_format: "float".to_string(),
        };

        let response = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(SemanticError::EmbeddingFailed(
                "Rate limited by OpenAI API".to_string(),
            ));
        }

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            if let Ok(error_response) = serde_json::from_str::<OpenAIErrorResponse>(&error_body) {
                return Err(SemanticError::EmbeddingFailed(error_response.error.message));
            }
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: OpenAIResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        api_response
            .data
            .into_iter()
            .next()
            .map(|d| d.embedding)
            .ok_or_else(|| SemanticError::EmbeddingFailed("Empty response from API".to_string()))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let request = OpenAIRequest {
            model: self.model.clone(),
            input: texts.iter().map(|s| s.to_string()).collect(),
            encoding_format: "float".to_string(),
        };

        let response = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: OpenAIResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        Ok(api_response.data.into_iter().map(|d| d.embedding).collect())
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        "openai-text-embedding-3-small"
    }
}

/// Cohere embedding provider
struct CohereProvider {
    api_key: String,
    endpoint: String,
    model: String,
    dimension: usize,
    client: reqwest::blocking::Client,
}

/// Cohere API request format
#[derive(serde::Serialize)]
struct CohereRequest {
    model: String,
    texts: Vec<String>,
    input_type: String,
    truncate: String,
}

/// Cohere API response format
#[derive(serde::Deserialize)]
struct CohereResponse {
    embeddings: Vec<Vec<f32>>,
}

impl CohereProvider {
    const DEFAULT_ENDPOINT: &'static str = "https://api.cohere.ai/v1/embed";
    const DEFAULT_MODEL: &'static str = "embed-english-v3.0";

    fn new(api_key: Option<String>, endpoint: Option<String>) -> Result<Self, SemanticError> {
        let api_key = api_key
            .ok_or_else(|| SemanticError::ConfigError("Cohere API key required".to_string()))?;

        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                SemanticError::ConfigError(format!("Failed to create HTTP client: {}", e))
            })?;

        Ok(Self {
            api_key,
            endpoint: endpoint.unwrap_or_else(|| Self::DEFAULT_ENDPOINT.to_string()),
            model: Self::DEFAULT_MODEL.to_string(),
            dimension: 1024,
            client,
        })
    }
}

impl EmbeddingProvider for CohereProvider {
    fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let request = CohereRequest {
            model: self.model.clone(),
            texts: vec![text.to_string()],
            input_type: "search_document".to_string(),
            truncate: "END".to_string(),
        };

        let response = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(SemanticError::EmbeddingFailed(
                "Rate limited by Cohere API".to_string(),
            ));
        }

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: CohereResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        api_response
            .embeddings
            .into_iter()
            .next()
            .ok_or_else(|| SemanticError::EmbeddingFailed("Empty response from API".to_string()))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let request = CohereRequest {
            model: self.model.clone(),
            texts: texts.iter().map(|s| s.to_string()).collect(),
            input_type: "search_document".to_string(),
            truncate: "END".to_string(),
        };

        let response = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: CohereResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        Ok(api_response.embeddings)
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        "cohere-embed-english-v3"
    }
}

/// Custom HTTP endpoint provider
struct CustomHttpProvider {
    endpoint: String,
    api_key: Option<String>,
    dimension: usize,
    client: reqwest::blocking::Client,
}

/// Generic custom request format
#[derive(serde::Serialize)]
struct CustomRequest {
    inputs: Vec<String>,
}

/// Generic custom response format - supports multiple formats
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum CustomResponse {
    /// Direct array of embeddings
    DirectArray(Vec<Vec<f32>>),
    /// Object with embeddings field
    WithEmbeddings { embeddings: Vec<Vec<f32>> },
    /// Object with data field (OpenAI-like)
    WithData { data: Vec<CustomEmbeddingItem> },
}

#[derive(serde::Deserialize)]
struct CustomEmbeddingItem {
    embedding: Vec<f32>,
}

impl CustomHttpProvider {
    fn new(endpoint: String, api_key: Option<String>) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            endpoint,
            api_key,
            dimension: 384, // Default, will be determined by actual embeddings
            client,
        }
    }
}

impl EmbeddingProvider for CustomHttpProvider {
    fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let request = CustomRequest {
            inputs: vec![text.to_string()],
        };

        let mut req_builder = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(api_key) = &self.api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = req_builder
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: CustomResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        let embeddings = match api_response {
            CustomResponse::DirectArray(embs) => embs,
            CustomResponse::WithEmbeddings { embeddings } => embeddings,
            CustomResponse::WithData { data } => data.into_iter().map(|d| d.embedding).collect(),
        };

        embeddings
            .into_iter()
            .next()
            .ok_or_else(|| SemanticError::EmbeddingFailed("Empty response from API".to_string()))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let request = CustomRequest {
            inputs: texts.iter().map(|s| s.to_string()).collect(),
        };

        let mut req_builder = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(api_key) = &self.api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = req_builder
            .send()
            .map_err(|e| SemanticError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().unwrap_or_default();
            return Err(SemanticError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: CustomResponse = response.json().map_err(|e| {
            SemanticError::EmbeddingFailed(format!("Failed to parse response: {}", e))
        })?;

        match api_response {
            CustomResponse::DirectArray(embs) => Ok(embs),
            CustomResponse::WithEmbeddings { embeddings } => Ok(embeddings),
            CustomResponse::WithData { data } => {
                Ok(data.into_iter().map(|d| d.embedding).collect())
            }
        }
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        "custom-http"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_provider() {
        let provider = PlaceholderProvider::new(384, "test".to_string());

        let embedding = provider.embed("test text").unwrap();
        assert_eq!(embedding.len(), 384);

        // Verify normalization
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);

        // Verify determinism
        let embedding2 = provider.embed("test text").unwrap();
        assert_eq!(embedding, embedding2);

        // Different text should produce different embedding
        let embedding3 = provider.embed("different text").unwrap();
        assert_ne!(embedding, embedding3);
    }

    #[test]
    fn test_embedding_model_placeholder() {
        let model = EmbeddingModel::placeholder(512);

        assert_eq!(model.dimension(), 512);
        assert_eq!(model.model_name(), "placeholder");

        let embedding = model.embed("test").unwrap();
        assert_eq!(embedding.len(), 512);
    }

    #[test]
    fn test_noop_provider() {
        let provider = NoOpProvider;

        let result = provider.embed("test");
        assert!(matches!(result, Err(SemanticError::EmbeddingFailed(_))));
        assert_eq!(provider.dimension(), 0);
    }

    #[test]
    fn test_embedding_batch() {
        let provider = PlaceholderProvider::new(384, "test".to_string());

        let texts = vec!["text1", "text2", "text3"];
        let embeddings = provider.embed_batch(&texts).unwrap();

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 384);
        }

        // Each should be different
        assert_ne!(embeddings[0], embeddings[1]);
        assert_ne!(embeddings[1], embeddings[2]);
    }
}
