//! Model downloading from Hugging Face.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

/// Hugging Face model repository
const REPO: &str = "sentence-transformers/all-MiniLM-L6-v2";

/// Files needed for the model
const MODEL_FILES: &[&str] = &[
    "config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "vocab.txt",
    "model.safetensors",
];

/// Download the all-MiniLM-L6-v2 model to the specified directory.
///
/// Returns the path to the model directory.
pub async fn download_model(model_dir: &Path) -> Result<PathBuf> {
    // Create model directory if needed
    if !model_dir.exists() {
        fs::create_dir_all(model_dir).await?;
        tracing::info!("Created model directory: {}", model_dir.display());
    }

    // Check if all files already exist
    let all_exist = check_model_files(model_dir).await;
    if all_exist {
        tracing::info!("Model already downloaded");
        return Ok(model_dir.to_path_buf());
    }

    tracing::info!("Downloading all-MiniLM-L6-v2 model from Hugging Face...");

    // Download each file
    for (idx, file) in MODEL_FILES.iter().enumerate() {
        let dest_path = model_dir.join(file);

        // Skip if already exists and is valid
        if dest_path.exists() {
            let metadata = fs::metadata(&dest_path).await?;
            if metadata.len() > 100 {
                tracing::debug!("{} already exists, skipping", file);
                continue;
            }
            // Remove invalid file
            fs::remove_file(&dest_path).await?;
        }

        let url = format!("https://huggingface.co/{}/resolve/main/{}", REPO, file);
        let progress_pct = ((idx + 1) * 100) / MODEL_FILES.len();
        tracing::info!(
            file = file,
            progress = format!("{}%", progress_pct),
            "Downloading model file"
        );

        download_file(&url, &dest_path)
            .await
            .with_context(|| format!("Failed to download {}", file))?;
    }

    // Optimize tokenizer.json by removing fixed padding
    optimize_tokenizer(model_dir).await?;

    tracing::info!("Model download complete!");
    Ok(model_dir.to_path_buf())
}

/// Check if all model files exist and are valid.
async fn check_model_files(model_dir: &Path) -> bool {
    for file in MODEL_FILES {
        let path = model_dir.join(file);
        match fs::metadata(&path).await {
            Ok(meta) if meta.len() > 100 => continue,
            _ => return false,
        }
    }
    true
}

/// Download a single file with redirect handling.
async fn download_file(url: &str, dest: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let response = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("HTTP error downloading {}", url))?;

    let total_size = response.content_length();
    let mut stream = response.bytes_stream();

    let mut file = File::create(dest).await?;
    let mut downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;

        // Log progress for large files
        if let Some(total) = total_size
            && total > 1_000_000
            && downloaded % 10_000_000 < chunk.len() as u64
        {
            let percent = (downloaded as f64 / total as f64) * 100.0;
            tracing::info!("  Progress: {:.1}%", percent);
        }
    }

    file.flush().await?;
    Ok(())
}

/// Remove fixed padding configuration from tokenizer.json.
///
/// Hugging Face's tokenizer.json contains `padding: { strategy: { Fixed: 128 } }`,
/// which pads all sequences to exactly 128 tokens. However, Python's sentence-transformers
/// library ignores this config and uses `padding=False` by default.
///
/// Candle's BERT implementation is sensitive to sequence length even with attention masking -
/// processing 128 positions produces different embeddings than processing the actual token count.
/// Removing the padding config aligns our behavior with Python's actual usage.
async fn optimize_tokenizer(model_dir: &Path) -> Result<()> {
    let tokenizer_path = model_dir.join("tokenizer.json");

    let content = fs::read_to_string(&tokenizer_path).await?;
    let mut data: serde_json::Value = serde_json::from_str(&content)?;

    if data.get("padding").is_some() {
        tracing::info!("Optimizing tokenizer.json - removing fixed padding configuration");

        if let Some(obj) = data.as_object_mut() {
            obj.remove("padding");
        }

        let optimized = serde_json::to_string_pretty(&data)?;
        fs::write(&tokenizer_path, optimized).await?;

        tracing::info!("Tokenizer optimized - now matches Python/PyTorch behavior");
    } else {
        tracing::debug!("Tokenizer already optimized");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_check_model_files_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        assert!(!check_model_files(temp_dir.path()).await);
    }

    #[tokio::test]
    async fn test_optimize_tokenizer_removes_padding() {
        let temp_dir = TempDir::new().unwrap();
        let tokenizer_path = temp_dir.path().join("tokenizer.json");

        // Create a tokenizer with padding config
        let tokenizer_json = r#"{
            "version": "1.0",
            "padding": {
                "strategy": { "Fixed": 128 }
            },
            "model": {}
        }"#;
        fs::write(&tokenizer_path, tokenizer_json).await.unwrap();

        // Optimize it
        optimize_tokenizer(temp_dir.path()).await.unwrap();

        // Check padding was removed
        let content = fs::read_to_string(&tokenizer_path).await.unwrap();
        let data: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(data.get("padding").is_none());
        assert!(data.get("version").is_some()); // Other fields preserved
    }
}
