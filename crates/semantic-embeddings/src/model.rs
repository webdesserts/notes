use anyhow::{Context, Result};
use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use sha2::{Digest, Sha256};

#[cfg(feature = "debug")]
use candle_core::IndexOp;
use candle_transformers::models::bert::{BertModel, Config};
use std::sync::Mutex;
use tokenizers::Tokenizer;

/// Version tag for the model fingerprint scheme. Bump when the fingerprint
/// inputs or their framing change.
const MODEL_FINGERPRINT_VERSION: &str = "semantic-embeddings-model-v1";

/// Compute a stable fingerprint over the exact model inputs (config JSON,
/// tokenizer JSON, weights bytes). Two loaders fed equivalent inputs produce
/// the same fingerprint regardless of whether the model came from disk or was
/// embedded in the binary.
fn compute_model_fingerprint(
    config_json: &str,
    tokenizer_json: &str,
    model_weights: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(MODEL_FINGERPRINT_VERSION.as_bytes());
    for input in [
        config_json.as_bytes(),
        tokenizer_json.as_bytes(),
        model_weights,
    ] {
        // Length-prefix each input so concatenation is unambiguous.
        hasher.update((input.len() as u64).to_le_bytes());
        hasher.update(input);
    }
    hex::encode(hasher.finalize())
}

/// Model manager handles loading and inference with the sentence transformer model
pub struct ModelManager {
    state: Mutex<Option<ModelState>>,
    /// Fingerprint of the model inputs for the currently loaded model.
    fingerprint: Mutex<Option<String>>,
}

struct ModelState {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
}

impl Default for ModelManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
            fingerprint: Mutex::new(None),
        }
    }

    /// Read-only identity of the currently loaded model, if any.
    ///
    /// Derived from the exact config/tokenizer/weights inputs passed to
    /// [`ModelManager::load_model`], so equivalent inputs always yield an
    /// equivalent fingerprint.
    pub fn model_fingerprint(&self) -> Option<String> {
        self.fingerprint.lock().unwrap().clone()
    }

    /// Initialize the model with data provided from JavaScript
    ///
    /// # Arguments
    /// * `config_json` - JSON string containing model config
    /// * `tokenizer_json` - JSON string containing tokenizer config
    /// * `model_weights` - Byte array containing model weights (safetensors format)
    pub fn load_model(
        &self,
        config_json: &str,
        tokenizer_json: &str,
        model_weights: &[u8],
    ) -> Result<()> {
        let mut state_guard = self.state.lock().unwrap();

        if state_guard.is_some() {
            return Ok(()); // Already loaded
        }

        let fingerprint = compute_model_fingerprint(config_json, tokenizer_json, model_weights);

        // Use CPU for now (Metal/CUDA support can be added later)
        let device = Device::Cpu;

        // Load tokenizer from JSON string
        let tokenizer = Tokenizer::from_bytes(tokenizer_json.as_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to load tokenizer: {}", e))?;

        // Parse model config
        let config: Config =
            serde_json::from_str(config_json).context("Failed to parse config.json")?;

        // Load model weights from bytes
        let vb = VarBuilder::from_buffered_safetensors(
            model_weights.to_vec(),
            candle_core::DType::F32,
            &device,
        )?;

        let model = BertModel::load(vb, &config)?;

        *state_guard = Some(ModelState {
            model,
            tokenizer,
            device,
        });
        *self.fingerprint.lock().unwrap() = Some(fingerprint);

        Ok(())
    }

    /// Check if model is loaded
    fn ensure_loaded(&self) -> Result<()> {
        let state_guard = self.state.lock().unwrap();

        if state_guard.is_none() {
            anyhow::bail!("Model not loaded. Call loadModel() first with model data.");
        }

        Ok(())
    }

    /// Encode a single text into embedding vector
    pub fn encode_single(&self, text: &str) -> Result<Vec<f32>> {
        self.ensure_loaded()?;

        let state_guard = self.state.lock().unwrap();
        let state = state_guard.as_ref().unwrap();

        // Tokenize
        let encoding = state
            .tokenizer
            .encode(text, true)
            .map_err(|e| anyhow::anyhow!("Tokenization failed: {}", e))?;

        let tokens: Vec<u32> = encoding.get_ids().to_vec();

        let token_ids = Tensor::new(&tokens[..], &state.device)?.unsqueeze(0)?; // Add batch dimension

        // Get attention mask from tokenizer encoding (marks real tokens as 1, padding as 0)
        // Note: Using U32 instead of F32 because BertModel internally converts to F32
        // via get_extended_attention_mask(). Some intermediate operations require integer dtype.
        let mask: Vec<u32> = encoding.get_attention_mask().to_vec();

        #[cfg(feature = "debug")]
        {
            let real_tokens = mask.iter().filter(|&&m| m == 1).count();
            log::debug!(
                "Tokenization: {} total tokens, {} real tokens (first 10 mask values: {:?})",
                mask.len(),
                real_tokens,
                &mask[..10.min(mask.len())]
            );
        }

        let attention_mask = Tensor::new(&mask[..], &state.device)?.unsqueeze(0)?; // Add batch dimension

        // Run model inference (token_type_ids = None for single sequence)
        let output = state.model.forward(&token_ids, &attention_mask, None)?;

        #[cfg(feature = "debug")]
        {
            // Log raw BERT output BEFORE pooling to verify where divergence occurs
            let shape = output.shape();
            log::debug!(
                "Raw BERT output shape: {:?} [batch, seq_len, hidden_dim]",
                shape.dims()
            );

            // Get first token ([CLS]) embeddings for first 10 dimensions
            let cls_token = output.i((0, 0, 0..10))?.to_vec1::<f32>()?;
            log::debug!("First token ([CLS]) dims 0-9: {:?}", cls_token);

            // Get second token embeddings for first 10 dimensions
            let token2 = output.i((0, 1, 0..10))?.to_vec1::<f32>()?;
            log::debug!("Second token dims 0-9: {:?}", token2);

            // Statistics across all tokens for dimension 0
            let dim0_all_tokens = output.i((0, .., 0))?.to_vec1::<f32>()?;
            let mean: f32 = dim0_all_tokens.iter().sum::<f32>() / dim0_all_tokens.len() as f32;
            log::debug!(
                "Dimension 0 across all tokens: mean={:.6}, values={:?}",
                mean,
                &dim0_all_tokens[..5.min(dim0_all_tokens.len())]
            );
        }

        // Convert attention mask to F32 for mean pooling
        let attention_mask_f32 = attention_mask.to_dtype(candle_core::DType::F32)?;

        // Mean pooling over sequence dimension
        let embedding = self.mean_pool(&output, &attention_mask_f32)?;

        // Normalize embedding
        let normalized = self.normalize(&embedding)?;

        #[cfg(feature = "debug")]
        {
            let norm_vec = normalized.squeeze(0)?.to_vec1::<f32>()?;
            let norm = norm_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            log::debug!("Embedding L2 norm: {:.6}", norm);
        }

        // Remove batch dimension and convert to Vec<f32>
        normalized
            .squeeze(0)?
            .to_vec1()
            .context("Failed to convert tensor to vec")
    }

    /// Encode multiple texts in batch
    pub fn encode_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        self.ensure_loaded()?;

        let state_guard = self.state.lock().unwrap();
        let state = state_guard.as_ref().unwrap();

        // Encode all texts
        let encodings: Vec<_> = texts
            .iter()
            .map(|text| {
                state
                    .tokenizer
                    .encode(text.as_str(), true)
                    .map_err(|e| anyhow::anyhow!("Tokenization failed: {}", e))
            })
            .collect::<Result<Vec<_>>>()?;

        // Find max length for padding
        let max_len = encodings.iter().map(|e| e.len()).max().unwrap_or(0);

        // Pad all sequences to same length and create batch tensors
        let mut all_token_ids = Vec::new();
        let mut all_masks = Vec::new();

        for encoding in &encodings {
            let tokens: Vec<u32> = encoding.get_ids().to_vec();
            let mut padded_tokens = tokens.clone();
            let mut mask = vec![1u32; tokens.len()];

            // Pad to max_len
            while padded_tokens.len() < max_len {
                padded_tokens.push(0); // PAD token
                mask.push(0);
            }

            all_token_ids.push(padded_tokens);
            all_masks.push(mask);
        }

        // Convert to tensors
        // Note: Attention mask created as U32 because BertModel requires integer dtype
        // for some internal operations before converting to F32 via get_extended_attention_mask()
        let token_ids = Tensor::new(all_token_ids, &state.device)?;
        let attention_mask = Tensor::new(all_masks, &state.device)?;

        // Run batch inference (token_type_ids = None for single sequence)
        let output = state.model.forward(&token_ids, &attention_mask, None)?;

        // Convert attention mask to F32 for mean pooling
        let attention_mask_f32 = attention_mask.to_dtype(candle_core::DType::F32)?;

        // Mean pooling for each item in batch
        let embeddings = self.mean_pool(&output, &attention_mask_f32)?;

        // Normalize embeddings
        let normalized = self.normalize(&embeddings)?;

        // Convert to Vec<Vec<f32>>
        let embedding_vecs: Vec<Vec<f32>> = (0..texts.len())
            .map(|i| {
                normalized
                    .get(i)
                    .ok()
                    .and_then(|tensor| tensor.to_vec1().ok())
                    .unwrap_or_default()
            })
            .collect();

        Ok(embedding_vecs)
    }

    /// Mean pooling: average token embeddings weighted by attention mask
    ///
    /// Uses mean pooling instead of [CLS] token because it better captures the
    /// overall meaning of the text by averaging information from all tokens.
    /// The attention mask weights ensure padding tokens don't affect the average.
    ///
    /// See: https://www.sbert.net/docs/usage/computing_sentence_embeddings.html
    fn mean_pool(&self, token_embeddings: &Tensor, attention_mask: &Tensor) -> Result<Tensor> {
        // token_embeddings shape: [batch_size, seq_len, hidden_size]
        // attention_mask shape: [batch_size, seq_len]

        // Expand mask to match embeddings: [batch_size, seq_len, hidden_size]
        // This matches the Python: attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        let dims = token_embeddings.dims();
        let mask_expanded = attention_mask
            .unsqueeze(2)?
            .broadcast_as((dims[0], dims[1], dims[2]))?
            .to_dtype(candle_core::DType::F32)?;

        // Multiply embeddings by mask
        let masked_embeddings = token_embeddings.mul(&mask_expanded)?;

        // Sum over sequence dimension (dim 1)
        let sum_embeddings = masked_embeddings.sum(1)?;

        // Sum the expanded mask over sequence dimension to get [batch_size, hidden_size]
        // This matches Python: input_mask_expanded.sum(1)
        let sum_mask = mask_expanded.sum(1)?;

        #[cfg(feature = "debug")]
        {
            // Verify that sum_mask equals the real token count (critical for bug detection)
            let mask_sum_vec = sum_mask.i(0)?.to_vec1::<f32>()?;
            let first_mask_sum = mask_sum_vec[0];
            log::debug!(
                "Mean pooling: sum_mask[0] = {:.1} (should equal real token count)",
                first_mask_sum
            );
        }

        // Divide to get mean, with clamping to prevent division by zero
        // Python uses: torch.clamp(sum_mask, min=1e-9)
        let sum_mask_clamped = sum_mask.clamp(1e-9, f64::MAX)?;

        let result = sum_embeddings.broadcast_div(&sum_mask_clamped)?;

        Ok(result)
    }

    /// Normalize embeddings to unit length (L2 normalization)
    ///
    /// Projects embeddings onto the unit hypersphere so cosine similarity becomes
    /// equivalent to dot product. This makes similarity computation faster and ensures
    /// embeddings are comparable regardless of input length.
    ///
    /// See: https://www.sbert.net/docs/usage/computing_sentence_embeddings.html
    fn normalize(&self, embeddings: &Tensor) -> Result<Tensor> {
        // Compute L2 norm for each embedding
        let norm = embeddings
            .sqr()?
            .sum_keepdim(embeddings.dims().len() - 1)?
            .sqrt()?;

        // Divide by norm
        embeddings
            .broadcast_div(&norm)
            .context("Normalization failed")
    }
}

// Unit tests are in model.rs, integration tests are in tests/ directory
