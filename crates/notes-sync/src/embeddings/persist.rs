//! Persistent embedding-cache envelope: codec, validation and atomic writes.
//!
//! The cache is derived, local and disposable: it lives in a memory-owned,
//! versioned file under `.obsidian/` and is separate from note truth and CRDT
//! state. Entries are only reused when the note content hash, the embedding
//! space fingerprint, the vector dimension and a usable vector norm all match;
//! anything else is discarded and recomputed. Missing or malformed files rebuild safely —
//! persistence problems never corrupt notes or fail a valid search.
//!
//! Independent processes may publish conservative last-writer-wins snapshots:
//! writes use unique same-directory temp files and atomic replacement, so a
//! reader always sees either the old complete snapshot or the new complete
//! snapshot, never a torn file.

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use semantic_embeddings::EMBEDDING_DIM;

/// On-disk envelope format version. Bump on breaking envelope changes.
pub const CACHE_FORMAT_VERSION: u32 = 1;
/// Version of the embedding + preprocessing pipeline that produced vectors.
/// Bump when tokenization/pooling behavior changes, even for the same model.
pub const EMBEDDING_PIPELINE_VERSION: u32 = 1;

/// Cache file name. Distinct from the legacy `embedding-cache.json`, which has
/// no model provenance; the legacy file is neither read nor written.
pub const CACHE_FILENAME: &str = "memory-embedding-cache-v1.json";

/// A single cached embedding: note content hash plus its vector.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    /// SHA-256 hash of the note content the vector was computed from.
    pub content_hash: String,
    /// The embedding vector.
    pub embedding: Vec<f32>,
}

/// Versioned envelope for the persisted embedding cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEnvelope {
    /// On-disk format version.
    pub format_version: u32,
    /// Embedding/preprocessing pipeline version.
    pub embedding_version: u32,
    /// Fingerprint of the exact model inputs (config/tokenizer/weights).
    pub model_fingerprint: String,
    /// Expected embedding dimension for every entry.
    pub dimension: usize,
    /// Path-keyed cache entries.
    pub entries: HashMap<String, CacheEntry>,
}

/// Outcome of validating a decoded envelope against the current model.
#[derive(Debug, Default)]
pub struct ValidatedCache {
    /// Entries safe to reuse (compatible envelope, well-formed records).
    pub entries: HashMap<String, CacheEntry>,
    /// Number of well-formed-envelope records dropped for being invalid
    /// (wrong dimension, non-finite values, zero/overflowing/underflowing
    /// norms).
    pub dropped_entries: usize,
}

/// Build an envelope from a path-keyed snapshot.
pub fn build_envelope(
    model_fingerprint: &str,
    entries: HashMap<String, CacheEntry>,
) -> CacheEnvelope {
    CacheEnvelope {
        format_version: CACHE_FORMAT_VERSION,
        embedding_version: EMBEDDING_PIPELINE_VERSION,
        model_fingerprint: model_fingerprint.to_string(),
        dimension: EMBEDDING_DIM,
        entries,
    }
}

/// Whether the envelope was produced by the current format, pipeline and
/// model identity. A mismatch means the whole snapshot is incompatible and
/// every entry must be recomputed.
fn envelope_identity_matches(envelope: &CacheEnvelope, expected_fingerprint: &str) -> bool {
    envelope.format_version == CACHE_FORMAT_VERSION
        && envelope.embedding_version == EMBEDDING_PIPELINE_VERSION
        && envelope.model_fingerprint == expected_fingerprint
        && envelope.dimension == EMBEDDING_DIM
}

/// Whether cosine similarity's actual arithmetic can use this vector. It
/// computes `sqrt(sum(x*x))` in f32; a norm of `0.0` (all-zero or fully
/// underflowed vector) makes every similarity come out `0.0`, and an infinite
/// norm (sum of squares overflowing f32) collapses the division to `0.0` too —
/// both silently erase search matches, so such entries are recomputed.
fn usable_norm(embedding: &[f32]) -> bool {
    let norm = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    norm.is_finite() && norm > 0.0
}

/// Validate a decoded envelope against the expected model identity.
///
/// An envelope from a different format version, pipeline version or model
/// fingerprint is rejected wholesale (full recomputation). Within a compatible
/// envelope, individual malformed records are dropped so only they are
/// recomputed — never silently trusted. Any note path is legal (including
/// names beginning with `__`): the retired synthetic `__query__` cache seam no
/// longer exists, so a leading `__` proves nothing about a record's origin.
pub fn validate_envelope(envelope: CacheEnvelope, expected_fingerprint: &str) -> ValidatedCache {
    let mut validated = ValidatedCache::default();

    if !envelope_identity_matches(&envelope, expected_fingerprint) {
        return validated;
    }

    for (path, entry) in envelope.entries {
        if entry.embedding.len() != envelope.dimension
            || entry.embedding.iter().any(|v| !v.is_finite())
            || !usable_norm(&entry.embedding)
        {
            validated.dropped_entries += 1;
            continue;
        }
        validated.entries.insert(path, entry);
    }

    validated
}

/// Decode and validate a cache file's contents.
///
/// Runs the JSON decode off the async executor via `spawn_blocking`; file I/O
/// is expected to have been done by the caller. Returns:
///
/// - `Ok(Some(validated))`: decodable envelope; `validated.entries` holds the
///   entries safe to reuse (possibly empty if records were malformed)
/// - `Ok(None)`: decodable JSON but incompatible envelope (rebuild everything)
/// - `Err(..)`: malformed JSON (rebuild everything, with logging at the call site)
pub async fn decode_cache(
    json: String,
    expected_fingerprint: &str,
) -> Result<Option<ValidatedCache>> {
    let expected_fingerprint = expected_fingerprint.to_string();
    tokio::task::spawn_blocking(move || {
        let envelope: CacheEnvelope = serde_json::from_str(&json)?;
        if !envelope_identity_matches(&envelope, &expected_fingerprint) {
            return Ok(None);
        }
        let validated = validate_envelope(envelope, &expected_fingerprint);
        Ok(Some(validated))
    })
    .await
    .map_err(|e| anyhow::anyhow!("cache decode task panicked: {}", e))?
}

/// Atomically persist an envelope: serialize off the async executor, write to
/// a unique same-directory temp file, fsync, then rename over the target.
///
/// Readers therefore see either the old complete snapshot or the new one, and
/// concurrent publishers degrade to last-writer-wins instead of torn files.
pub async fn encode_and_write_cache(path: &Path, envelope: &CacheEnvelope) -> Result<()> {
    let path = path.to_path_buf();
    let envelope = envelope.clone();
    let json = tokio::task::spawn_blocking(move || serde_json::to_string(&envelope))
        .await
        .map_err(|e| anyhow::anyhow!("cache encode task panicked: {}", e))??;

    write_atomically(&path, json).await
}

/// Write `contents` to `path` via a unique sibling temp file + atomic rename.
async fn write_atomically(path: &Path, contents: String) -> Result<()> {
    let path = path.to_path_buf();
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cache path has no parent directory: {}", path.display()))?
        .to_path_buf();
    tokio::fs::create_dir_all(&parent).await?;

    // All blocking work — unique temp-name generation, write, fsync and the
    // atomic rename — happens on the blocking pool, never on the executor.
    let temp_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        use std::io::Write;
        let temp = tempfile::Builder::new()
            .prefix(".memory-cache-")
            .suffix(".tmp")
            .tempfile_in(&parent)?;
        let temp_path = temp.path().to_path_buf();

        {
            let mut file = temp.as_file();
            file.write_all(contents.as_bytes())?;
            file.flush()?;
            temp.as_file().sync_all()?;
        }

        temp.persist(&path)?;
        Ok(temp_path)
    })
    .await
    .map_err(|e| anyhow::anyhow!("cache write task panicked: {}", e))??;

    tracing::trace!(temp = %temp_path.display(), "Persisted embedding cache snapshot atomically");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FP: &str = "fp-test-1234";

    fn entry(values: &[f32]) -> CacheEntry {
        CacheEntry {
            content_hash: "hash".to_string(),
            embedding: values.to_vec(),
        }
    }

    /// Well-formed record at the real embedding dimension.
    fn dim_entry(seed: f32) -> CacheEntry {
        entry(&vec![seed; EMBEDDING_DIM])
    }

    fn valid_envelope() -> CacheEnvelope {
        let mut entries = HashMap::new();
        entries.insert("notes/a.md".to_string(), dim_entry(0.1));
        build_envelope(FP, entries)
    }

    #[test]
    fn test_envelope_json_roundtrip() {
        let envelope = valid_envelope();
        let json = serde_json::to_string(&envelope).unwrap();
        let decoded: CacheEnvelope = serde_json::from_str(&json).unwrap();
        let validated = validate_envelope(decoded, FP);
        assert_eq!(validated.entries.len(), 1);
        assert_eq!(validated.dropped_entries, 0);
        assert_eq!(
            validated.entries["notes/a.md"].embedding,
            vec![0.1; EMBEDDING_DIM]
        );
    }

    #[tokio::test]
    async fn test_decode_rejects_malformed_json() {
        let result = decode_cache("not json at all".to_string(), FP).await;
        assert!(result.is_err(), "malformed JSON must be an error (rebuild)");
    }

    #[test]
    fn test_validate_rejects_wrong_fingerprint() {
        let mut envelope = valid_envelope();
        envelope.model_fingerprint = "different-model".to_string();
        let validated = validate_envelope(envelope, FP);
        assert!(validated.entries.is_empty());
        assert_eq!(validated.dropped_entries, 0);
    }

    #[test]
    fn test_validate_rejects_wrong_format_version() {
        let mut envelope = valid_envelope();
        envelope.format_version = CACHE_FORMAT_VERSION + 1;
        let validated = validate_envelope(envelope, FP);
        assert!(validated.entries.is_empty());
    }

    #[test]
    fn test_validate_rejects_wrong_pipeline_version() {
        let mut envelope = valid_envelope();
        envelope.embedding_version = EMBEDDING_PIPELINE_VERSION + 1;
        let validated = validate_envelope(envelope, FP);
        assert!(validated.entries.is_empty());
    }

    #[test]
    fn test_validate_rejects_wrong_dimension() {
        let mut envelope = valid_envelope();
        envelope.dimension = 7;
        let validated = validate_envelope(envelope, FP);
        assert!(validated.entries.is_empty());
    }

    #[test]
    fn test_validate_drops_malformed_records() {
        let mut entries = HashMap::new();
        entries.insert("notes/good.md".to_string(), dim_entry(0.1));
        entries.insert("notes/wrongdim.md".to_string(), entry(&[0.1, 0.2]));
        let mut nonfinite = dim_entry(0.3);
        nonfinite.embedding[7] = f32::NAN;
        entries.insert("notes/nonfinite.md".to_string(), nonfinite);
        let mut infinite = dim_entry(0.4);
        infinite.embedding[7] = f32::INFINITY;
        entries.insert("notes/infinite.md".to_string(), infinite);

        let validated = validate_envelope(build_envelope(FP, entries), FP);
        assert_eq!(validated.entries.len(), 1);
        assert!(validated.entries.contains_key("notes/good.md"));
        assert_eq!(validated.dropped_entries, 3);
    }

    /// A path beginning with `__` is a legal note name, not evidence of a
    /// synthetic query entry: such records must be reused like any other.
    #[test]
    fn test_validate_accepts_dunder_prefixed_note_names() {
        let mut entries = HashMap::new();
        entries.insert("__valid-note.md".to_string(), dim_entry(0.1));
        entries.insert("__query__".to_string(), dim_entry(0.2));

        let validated = validate_envelope(build_envelope(FP, entries), FP);
        assert_eq!(validated.entries.len(), 2);
        assert_eq!(validated.dropped_entries, 0);
    }

    /// Vectors whose cosine-similarity arithmetic is degenerate — zero norm,
    /// finite elements whose f32 sum of squares overflows to +inf, and finite
    /// elements whose squares all underflow to zero — are dropped and
    /// recomputed instead of silently erasing search matches.
    #[test]
    fn test_validate_rejects_zero_overflow_and_underflow_norms() {
        let mut entries = HashMap::new();
        entries.insert("notes/good.md".to_string(), dim_entry(0.1));
        entries.insert("notes/zero.md".to_string(), dim_entry(0.0));
        entries.insert(
            "notes/overflow.md".to_string(),
            entry(&vec![1e20; EMBEDDING_DIM]),
        );
        entries.insert(
            "notes/underflow.md".to_string(),
            entry(&vec![1e-25; EMBEDDING_DIM]),
        );

        let validated = validate_envelope(build_envelope(FP, entries), FP);
        assert_eq!(validated.entries.len(), 1);
        assert!(validated.entries.contains_key("notes/good.md"));
        assert_eq!(validated.dropped_entries, 3);
    }

    #[tokio::test]
    async fn test_write_then_decode_roundtrip_on_disk() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(CACHE_FILENAME);

        encode_and_write_cache(&path, &valid_envelope())
            .await
            .unwrap();
        assert!(path.exists());
        // No temp litter left behind.
        let leftovers: Vec<_> = std::fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");

        let json = tokio::fs::read_to_string(&path).await.unwrap();
        let validated = decode_cache(json, FP).await.unwrap().unwrap();
        assert_eq!(validated.entries.len(), 1);
    }

    #[tokio::test]
    async fn test_decode_truncated_write_is_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(CACHE_FILENAME);
        encode_and_write_cache(&path, &valid_envelope())
            .await
            .unwrap();

        // Simulate an interrupted write: truncate the JSON mid-structure.
        let json = tokio::fs::read_to_string(&path).await.unwrap();
        let truncated = &json[..json.len() / 2];
        let result = decode_cache(truncated.to_string(), FP).await;
        assert!(result.is_err(), "truncated snapshot must not validate");
    }

    #[tokio::test]
    async fn test_decode_incompatible_envelope_is_none() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(CACHE_FILENAME);
        encode_and_write_cache(&path, &valid_envelope())
            .await
            .unwrap();

        let json = tokio::fs::read_to_string(&path).await.unwrap();
        let result = decode_cache(json, "some-other-model").await.unwrap();
        assert!(
            result.is_none(),
            "incompatible envelope must be rejected wholesale"
        );
    }

    /// Concurrent publishers of different snapshots to the same path must
    /// never produce a torn file: every reader sees a complete snapshot, and
    /// the final file is one of the published snapshots (last-writer-wins).
    #[tokio::test]
    async fn test_concurrent_publishers_never_tear() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(CACHE_FILENAME);

        let mut handles = Vec::new();
        for writer in 0..8 {
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                for round in 0..5 {
                    let mut entries = HashMap::new();
                    entries.insert(format!("notes/w{writer}-{round}.md"), dim_entry(0.25));
                    encode_and_write_cache(&path, &build_envelope(FP, entries))
                        .await
                        .unwrap();

                    // Interleaved reader: whatever it sees must fully decode.
                    if path.exists()
                        && let Ok(json) = tokio::fs::read_to_string(&path).await
                    {
                        let decoded: Result<CacheEnvelope, _> = serde_json::from_str(&json);
                        assert!(decoded.is_ok(), "reader observed a torn snapshot: {json:?}");
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let json = tokio::fs::read_to_string(&path).await.unwrap();
        let final_envelope: CacheEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(final_envelope.entries.len(), 1);
    }
}
