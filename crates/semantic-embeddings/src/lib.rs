#![deny(clippy::all)]

mod embedding;
mod model;

#[cfg(feature = "embedded-model")]
mod embedded;

// Re-export for external use
pub use embedding::{cosine_similarity, find_most_similar};
pub use model::ModelManager;

/// Embedding dimension for all-MiniLM-L6-v2 model.
/// This is determined by the model architecture's hidden size.
pub const EMBEDDING_DIM: usize = 384;

/// Type alias for an embedding vector.
pub type Embedding = Vec<f32>;

// Auto-initialize logging for debug builds
#[cfg(feature = "debug")]
#[ctor::ctor]
fn init_native_logging() {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

/// Semantic embedding generator for text content.
///
/// Wraps ModelManager with a convenient API for loading models from files
/// and generating embeddings.
///
/// # Example
/// ```ignore
/// use semantic_embeddings::SemanticEmbeddings;
/// use std::path::Path;
///
/// let embeddings = SemanticEmbeddings::new();
/// embeddings.load_model_from_dir(Path::new("models/all-MiniLM-L6-v2"))?;
///
/// let embedding = embeddings.encode("Hello world")?;
/// println!("Embedding dimension: {}", embedding.len());
/// ```
pub struct SemanticEmbeddings {
    model: ModelManager,
}

impl SemanticEmbeddings {
    /// Create a new SemanticEmbeddings instance.
    ///
    /// Call `load_model_from_dir()` to initialize with model data before using `encode()`.
    pub fn new() -> Self {
        Self {
            model: ModelManager::new(),
        }
    }

    /// Load model from a directory containing config.json, tokenizer.json, and model.safetensors.
    ///
    /// # Arguments
    /// * `model_dir` - Path to directory containing model files
    ///
    /// # Expected files
    /// - `config.json` - Model configuration
    /// - `tokenizer.json` - Tokenizer configuration
    /// - `model.safetensors` - Model weights
    pub fn load_model_from_dir(&self, model_dir: &std::path::Path) -> anyhow::Result<()> {
        use std::fs;

        let config_path = model_dir.join("config.json");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let weights_path = model_dir.join("model.safetensors");

        let config_json = fs::read_to_string(&config_path)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", config_path.display(), e))?;

        let tokenizer_json = fs::read_to_string(&tokenizer_path)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", tokenizer_path.display(), e))?;

        let model_weights = fs::read(&weights_path)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", weights_path.display(), e))?;

        self.model
            .load_model(&config_json, &tokenizer_json, &model_weights)
    }

    /// Load model from provided data (for cases where you have the data in memory).
    ///
    /// # Arguments
    /// * `config_json` - JSON string containing model config
    /// * `tokenizer_json` - JSON string containing tokenizer config
    /// * `model_weights` - Byte slice containing model weights (safetensors format)
    pub fn load_model(
        &self,
        config_json: &str,
        tokenizer_json: &str,
        model_weights: &[u8],
    ) -> anyhow::Result<()> {
        self.model
            .load_model(config_json, tokenizer_json, model_weights)
    }

    /// Load the embedded model (when compiled with `embedded-model` feature).
    ///
    /// This is the preferred method for release builds - no network access required.
    /// The model files are baked into the binary at compile time.
    ///
    /// # Errors
    /// Returns an error if model loading fails (should not happen with valid embedded data).
    #[cfg(feature = "embedded-model")]
    pub fn load_embedded_model(&self) -> anyhow::Result<()> {
        self.model.load_model(
            embedded::CONFIG_JSON,
            embedded::TOKENIZER_JSON,
            embedded::MODEL_WEIGHTS,
        )
    }

    /// Identity fingerprint of the currently loaded model, if any.
    ///
    /// Derived from the exact config/tokenizer/weights inputs, so embedded and
    /// runtime-file loaders produce equivalent fingerprints for equivalent
    /// inputs. Returns `None` before a model is loaded.
    pub fn model_fingerprint(&self) -> Option<String> {
        self.model.model_fingerprint()
    }

    /// Encode a single text into an embedding vector.
    ///
    /// Returns a Vec<f32> of dimension 384 (for all-MiniLM-L6-v2).
    pub fn encode(&self, text: &str) -> anyhow::Result<Vec<f32>> {
        self.model.encode_single(text)
    }

    /// Encode multiple texts in batch (more efficient than multiple encode() calls).
    ///
    /// Returns Vec of embedding vectors.
    pub fn encode_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        self.model.encode_batch(texts)
    }

    /// Compute cosine similarity between two embedding vectors.
    ///
    /// Returns a value between -1 and 1 (typically 0 to 1 for similar texts).
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> anyhow::Result<f32> {
        cosine_similarity(a, b)
    }

    /// Find indices of top K most similar embeddings to query.
    ///
    /// Returns array of indices sorted by similarity (descending).
    pub fn find_most_similar(
        query: &[f32],
        candidates: &[Vec<f32>],
        top_k: usize,
    ) -> anyhow::Result<Vec<u32>> {
        find_most_similar(query, candidates, top_k)
    }
}

impl Default for SemanticEmbeddings {
    fn default() -> Self {
        Self::new()
    }
}
