//! Embedding manager for generating and caching note embeddings.

use anyhow::{Context, Result};
use semantic_embeddings::SemanticEmbeddings;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

#[cfg(not(feature = "embedded-model"))]
use super::download::download_model;

use super::persist::{
    CACHE_FILENAME, CacheEntry, ValidatedCache, build_envelope, decode_cache,
    encode_and_write_cache,
};

/// Manages semantic embeddings for notes.
///
/// Handles model loading, embedding generation, and caching.
///
/// The note-embedding cache is persisted across processes: it loads on
/// initialization and, whenever a completed batch completes while the cache
/// holds unpersisted mutations (from the batch itself or an earlier
/// invalidation), publishes one atomic snapshot to disk.
/// Entries are only reused when note content, model identity and vector
/// dimension all match; anything else is recomputed. Query embeddings are
/// never cached or persisted (see [`Self::encode_query_texts`]).
pub struct EmbeddingManager {
    /// The embedding model
    embeddings: Arc<SemanticEmbeddings>,
    /// Cache of note embeddings: note_path -> (content_hash, embedding)
    cache: RwLock<HashMap<String, CacheEntry>>,
    /// Path to the cache file
    cache_path: PathBuf,
    /// Whether the model is loaded
    model_loaded: RwLock<bool>,
    /// Path to the model directory (used by the runtime HuggingFace download path).
    /// With `embedded-model` the model is baked into the binary, so this is unused in that build.
    #[cfg_attr(feature = "embedded-model", allow(dead_code))]
    model_dir: PathBuf,
    /// Monotonic generation counter of cache mutations (inserts and
    /// invalidations). A snapshot commit records the generation it actually
    /// captured, so any later mutation is detectable by plain inequality
    /// between the two counters — committing generation G can never clear
    /// the unpersisted state of a mutation from G+1.
    mutation_seq: AtomicU64,
    /// Generation captured by the last successful snapshot publish. The cache
    /// has unpersisted mutations exactly when `mutation_seq != persisted_seq`.
    persisted_seq: AtomicU64,
    /// Serializes snapshot commits within this process so an earlier snapshot
    /// cannot overwrite a later one.
    flush_lock: Mutex<()>,
    /// Number of note texts sent to the model for encoding since construction.
    /// Diagnostic only: lets tests prove compatible restarts re-encode nothing.
    encoded_text_count: AtomicU64,
    /// Test-only deterministic-interleaving gate for [`Self::flush_snapshot`]:
    /// when armed, the flush sends on the first channel when it reaches the
    /// point between the atomic file write and the generation commit, then
    /// waits on the second before continuing. Lets tests inject a mutation
    /// into exactly the window where the old read-then-clear dirtiness check
    /// could lose it.
    #[cfg(test)]
    flush_gate: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl EmbeddingManager {
    /// Create a new embedding manager.
    ///
    /// The model will be downloaded automatically if not present.
    pub fn new(vault_path: &Path) -> Self {
        let model_dir = vault_path.join(".obsidian/models/all-MiniLM-L6-v2");
        let cache_path = vault_path.join(".obsidian").join(CACHE_FILENAME);

        Self {
            embeddings: Arc::new(SemanticEmbeddings::new()),
            cache: RwLock::new(HashMap::new()),
            cache_path,
            model_loaded: RwLock::new(false),
            model_dir,
            mutation_seq: AtomicU64::new(0),
            persisted_seq: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
            encoded_text_count: AtomicU64::new(0),
            #[cfg(test)]
            flush_gate: std::sync::Mutex::new(None),
        }
    }

    /// Initialize the embedding manager by loading the model.
    ///
    /// With `embedded-model` feature: loads model from binary (no network).
    /// Without: downloads from HuggingFace if not already cached on disk.
    ///
    /// Uses write lock for the entire operation to prevent race conditions.
    pub async fn initialize(&self) -> Result<()> {
        // Hold write lock for entire initialization to prevent TOCTOU race
        let mut loaded = self.model_loaded.write().await;
        if *loaded {
            return Ok(());
        }

        #[cfg(feature = "embedded-model")]
        {
            self.embeddings
                .load_embedded_model()
                .context("Failed to load embedded model")?;
            tracing::info!("Loaded embedded model");
        }

        #[cfg(not(feature = "embedded-model"))]
        {
            download_model(&self.model_dir).await?;
            self.embeddings
                .load_model_from_dir(&self.model_dir)
                .context("Failed to load embedding model")?;
            tracing::info!("Loaded model from disk");
        }

        *loaded = true;

        // Load cache from disk
        self.load_cache().await?;

        tracing::info!("Embedding manager initialized");
        Ok(())
    }

    /// Ensure the model is loaded before use.
    async fn ensure_loaded(&self) -> Result<()> {
        if !*self.model_loaded.read().await {
            self.initialize().await?;
        }
        Ok(())
    }

    /// Encode texts for a search query without touching the note cache.
    ///
    /// Query embeddings are deliberately kept out of the persisted note index:
    /// they are ephemeral, not content-addressed by note path, and persisting
    /// them would dirty the snapshot on every search.
    pub async fn encode_query_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.ensure_loaded().await?;
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            let embedding = self.embeddings.encode(text)?;
            self.encoded_text_count.fetch_add(1, Ordering::Relaxed);
            results.push(embedding);
        }
        Ok(results)
    }

    /// Get embeddings for multiple notes in batch.
    ///
    /// Cached entries are reused when the note content hash matches; misses
    /// are computed in chunks. On successful completion the cache is persisted
    /// if this batch mutated it — or if a prior invalidation (note edit or
    /// delete) left it dirty, which the next completed batch flushes even when
    /// that batch had only cache hits.
    pub async fn get_embeddings_batch(
        &self,
        notes: &[(String, String)], // (path, content)
    ) -> Result<Vec<(String, Vec<f32>)>> {
        self.ensure_loaded().await?;

        let mut results = Vec::with_capacity(notes.len());
        let mut to_compute: Vec<(String, String, String)> = Vec::new(); // (path, content, content_hash)

        // Check cache for each note
        {
            let cache = self.cache.read().await;
            for (path, content) in notes.iter() {
                let content_hash = compute_hash(content);

                if let Some(entry) = cache.get(path)
                    && entry.content_hash == content_hash
                {
                    results.push((path.clone(), entry.embedding.clone()));
                    continue;
                }

                // Need to compute this one - store hash to avoid recomputing later
                to_compute.push((path.clone(), content.clone(), content_hash));
            }
        }

        // Batch compute embeddings for cache misses in chunks to limit memory usage
        if !to_compute.is_empty() {
            let total = to_compute.len();
            tracing::info!(
                "Computing {} embeddings in chunks ({} cached)",
                total,
                results.len()
            );

            const CHUNK_SIZE: usize = 25;

            let mut computed = 0;
            for chunk in to_compute.chunks(CHUNK_SIZE) {
                let texts: Vec<String> = chunk
                    .iter()
                    .map(|(_, content, _)| content.clone())
                    .collect();
                let embeddings = self.embeddings.encode_batch(&texts)?;
                self.encoded_text_count
                    .fetch_add(texts.len() as u64, Ordering::Relaxed);

                let mut cache = self.cache.write().await;
                for ((path, _, content_hash), embedding) in chunk.iter().zip(embeddings) {
                    cache.insert(
                        path.clone(),
                        CacheEntry {
                            content_hash: content_hash.clone(),
                            embedding: embedding.clone(),
                        },
                    );
                    results.push((path.clone(), embedding));
                    computed += 1;
                }
                // Record the mutation while holding the cache lock so
                // dirtiness and snapshot contents stay consistent.
                self.record_mutation(&cache);

                tracing::debug!("Computed {}/{} embeddings", computed, total);
            }

            tracing::debug!(cache_size = results.len(), "Embedding computation complete");
        } else {
            tracing::debug!(cache_hits = results.len(), "All embeddings from cache");
        }

        // Persist once at successful completion of the batch while the cache
        // holds unpersisted mutations — whether this batch mutated it or an
        // earlier invalidation did.
        if self.has_unpersisted_mutations()
            && let Err(e) = self.flush_snapshot().await
        {
            // Persistence problems degrade performance (next start
            // recomputes); they never fail an otherwise valid batch.
            tracing::warn!(
                "Failed to persist embedding cache: {}. Next start will recompute.",
                e
            );
        }

        Ok(results)
    }

    /// Record a cache mutation while the caller holds the cache write lock,
    /// keeping the generation coherent with the changed entries.
    fn record_mutation(&self, _cache: &HashMap<String, CacheEntry>) {
        self.mutation_seq.fetch_add(1, Ordering::Release);
    }

    /// Whether any completed mutation is not yet reflected in a persisted
    /// snapshot: the generation counters have diverged. A pure comparison —
    /// never a read-then-clear — so a writer cannot slip between the check
    /// and the bookkeeping.
    fn has_unpersisted_mutations(&self) -> bool {
        self.mutation_seq.load(Ordering::Acquire) != self.persisted_seq.load(Ordering::Acquire)
    }

    /// Publish one consistent snapshot of the cache to disk.
    ///
    /// Snapshots are serialized in-process via [`Self::flush_lock`], cloned
    /// under a short read lock (never held across I/O), and encoded/written
    /// off the async executor. The commit records the generation captured with
    /// the snapshot; mutations that race the write leave the generation
    /// counters unequal, so the next completed batch flushes again. There is
    /// no read-then-clear step for a writer to slip into. Concurrent processes
    /// publishing to the same file degrade to last-writer-wins via atomic
    /// replacement.
    async fn flush_snapshot(&self) -> Result<()> {
        let _guard = self.flush_lock.lock().await;

        let fingerprint = self
            .embeddings
            .model_fingerprint()
            .context("model fingerprint unavailable; cannot persist embedding cache")?;

        // Capture the snapshot and its generation coherently under one short
        // read lock: a concurrent batch holds the write lock across both its
        // insert and its `record_mutation` bump, so this pair is never torn.
        let (snapshot, seq_at_snapshot) = {
            let cache = self.cache.read().await;
            let seq = self.mutation_seq.load(Ordering::Acquire);
            (cache.clone(), seq)
        };

        let envelope = build_envelope(&fingerprint, snapshot);
        encode_and_write_cache(&self.cache_path, &envelope).await?;

        // Test-only gate pausing between the write and the commit, exposing
        // exactly the interleaving window (see `flush_gate`).
        #[cfg(test)]
        let flush_gate = self.flush_gate.lock().unwrap().take();
        #[cfg(test)]
        if let Some((reached_tx, release_rx)) = flush_gate {
            let _ = reached_tx.send(());
            let _ = release_rx.await;
        }

        // Commit records only the generation actually captured. Anything that
        // mutated after the capture — including while the file was being
        // written — keeps the counters unequal, which
        // [`Self::has_unpersisted_mutations`] detects by comparison alone.
        self.persisted_seq.store(seq_at_snapshot, Ordering::Release);

        tracing::debug!(
            entries = envelope.entries.len(),
            "Persisted embedding cache snapshot"
        );
        Ok(())
    }

    /// Load cache from disk, falling back to an empty cache on any problem.
    ///
    /// Missing, incompatible, malformed or unreadable caches all rebuild
    /// safely with diagnostic logging. The file is never deleted here: another
    /// process may be replacing it, and the next snapshot publish overwrites
    /// atomically anyway.
    async fn load_cache(&self) -> Result<()> {
        if !self.cache_path.exists() {
            tracing::debug!("No embedding cache on disk; starting with empty cache");
            return Ok(());
        }

        let Some(fingerprint) = self.embeddings.model_fingerprint() else {
            tracing::warn!("Model fingerprint unavailable; skipping embedding cache load");
            return Ok(());
        };

        let json = match fs::read_to_string(&self.cache_path).await {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!(
                    "Failed to read embedding cache: {}. Starting with empty cache.",
                    e
                );
                return Ok(());
            }
        };

        match decode_cache(json, &fingerprint).await {
            Ok(Some(ValidatedCache {
                entries,
                dropped_entries,
            })) => {
                let mut cache = self.cache.write().await;
                if dropped_entries > 0 {
                    tracing::warn!(
                        dropped = dropped_entries,
                        "Dropped malformed entries from embedding cache; they will be recomputed"
                    );
                    // Mark dirty so the next completed batch republishes a
                    // cleaned snapshot instead of leaving malformed records
                    // on disk forever.
                    self.record_mutation(&cache);
                }
                *cache = entries;
                tracing::debug!("Loaded embedding cache ({} entries)", cache.len());
            }
            Ok(None) => {
                tracing::info!(
                    "Embedding cache is incompatible (different model or format); rebuilding"
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to decode embedding cache: {}. Starting with empty cache.",
                    e
                );
            }
        }

        Ok(())
    }

    /// Invalidate cache entry for a note (edit or delete).
    ///
    /// Removes the in-memory entry and marks the cache dirty; the next
    /// completed batch persists the snapshot. No per-event disk write and no
    /// debounce timer: a burst of watcher invalidations costs O(1) flushes.
    pub async fn invalidate(&self, note_path: &str) {
        let mut cache = self.cache.write().await;
        if cache.remove(note_path).is_some() {
            self.record_mutation(&cache);
        }
    }

    /// Number of note texts sent to the model for encoding since construction.
    /// Diagnostic counter: production code never reads it, but it lets tests
    /// and tooling prove compatible restarts re-encode nothing. (The counter
    /// itself is always maintained.)
    #[allow(dead_code)]
    pub fn encoded_text_count(&self) -> u64 {
        self.encoded_text_count.load(Ordering::Relaxed)
    }

    /// Compute cosine similarity between two embeddings.
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
        SemanticEmbeddings::cosine_similarity(a, b)
    }
}

/// Compute SHA-256 hash of content.
fn compute_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash() {
        let hash1 = compute_hash("hello world");
        let hash2 = compute_hash("hello world");
        let hash3 = compute_hash("different content");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_eq!(hash1.len(), 64); // SHA-256 hex = 64 chars
    }
}

/// Model-bearing tests for cache persistence and recovery. These exercise the
/// real embedded model (assets checked in under
/// `crates/semantic-embeddings/models/`; never downloaded) through the real
/// `EmbeddingManager` seam, so they only compile and run when the
/// `embedded-model` feature is enabled:
///
/// ```text
/// cargo test -p memory --features embedded-model
/// ```
#[cfg(all(test, feature = "embedded-model"))]
mod persistence_tests {
    use super::*;
    use crate::embeddings::persist::{
        CACHE_FILENAME, CACHE_FORMAT_VERSION, CacheEnvelope, EMBEDDING_PIPELINE_VERSION,
    };
    use semantic_embeddings::EMBEDDING_DIM;
    use std::sync::Arc;

    fn note(path: &str, content: &str) -> (String, String) {
        (path.to_string(), content.to_string())
    }

    fn demo_notes() -> Vec<(String, String)> {
        vec![
            note(
                "knowledge/rust.md",
                "Rust is a systems programming language focused on safety and speed. ",
            ),
            note(
                "knowledge/gardening.md",
                "Tomatoes need full sun and consistent watering to thrive.",
            ),
            note(
                "journal/2026-09-04.md",
                "Spent the morning debugging the embedding cache and walking the dog.",
            ),
        ]
    }

    async fn new_manager(vault: &Path) -> Arc<EmbeddingManager> {
        let manager = Arc::new(EmbeddingManager::new(vault));
        manager
            .initialize()
            .await
            .expect("model + cache initialization must succeed");
        manager
    }

    fn read_envelope(vault: &Path) -> CacheEnvelope {
        let path = vault.join(".obsidian").join(CACHE_FILENAME);
        let json = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cache file must exist at {}: {}", path.display(), e));
        serde_json::from_str(&json).expect("persisted cache must decode as the v1 envelope")
    }

    fn write_envelope(vault: &Path, envelope: &CacheEnvelope) {
        let path = vault.join(".obsidian").join(CACHE_FILENAME);
        std::fs::write(&path, serde_json::to_string(envelope).unwrap()).unwrap();
    }

    /// A fresh manager over the same vault must reuse persisted embeddings for
    /// unchanged notes: no note text is re-encoded after restart (query
    /// encoding is the only other legitimate encode caller, and none happens
    /// here).
    #[tokio::test]
    async fn test_fresh_manager_reuses_persisted_embeddings_without_reencoding() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let first = new_manager(vault.path()).await;
        let first_results = first.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(first.encoded_text_count(), 3);
        assert!(vault.path().join(".obsidian").join(CACHE_FILENAME).exists());
        drop(first);

        let second = new_manager(vault.path()).await;
        let second_results = second.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(
            second.encoded_text_count(),
            0,
            "unchanged compatible notes must cause zero encoding calls after restart"
        );
        assert_eq!(first_results, second_results);

        // And the reused vectors match what was persisted.
        let envelope = read_envelope(vault.path());
        for (path, embedding) in &second_results {
            assert_eq!(
                envelope.entries[path].embedding, *embedding,
                "reused embedding for {path} must equal the persisted one"
            );
        }
    }

    /// Edited content is re-encoded and the snapshot reflects the new hash;
    /// deleted notes disappear from search results and, after an invalidation
    /// plus the next completed batch, from the persisted snapshot too.
    #[tokio::test]
    async fn test_edits_and_deletes_are_current_after_restart() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let first = new_manager(vault.path()).await;
        first.get_embeddings_batch(&notes).await.unwrap();
        drop(first);

        // Edit one note, drop another (simulating the watcher invalidation for
        // the deletion on the second manager).
        let mut after = demo_notes();
        after[0] = note(
            "knowledge/rust.md",
            "Rust ownership makes data races impossible at compile time.",
        );
        after.truncate(2); // "journal/2026-09-04.md" deleted

        let second = new_manager(vault.path()).await;
        second.invalidate("journal/2026-09-04.md").await;
        let results = second.get_embeddings_batch(&after).await.unwrap();

        assert_eq!(
            second.encoded_text_count(),
            1,
            "only the edited note is re-encoded"
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(p, _)| p != "journal/2026-09-04.md"));

        // The completed batch flushed the invalidation's dirtiness: the
        // snapshot no longer holds the deleted note.
        let envelope = read_envelope(vault.path());
        assert_eq!(envelope.entries.len(), 2);
        assert!(!envelope.entries.contains_key("journal/2026-09-04.md"));
        let edited_hash = compute_hash(&after[0].1);
        assert_eq!(
            envelope.entries["knowledge/rust.md"].content_hash,
            edited_hash
        );
    }

    /// A cache-hit-only completed batch must still flush dirtiness left behind
    /// by an invalidation burst — one flush, no per-event writes.
    #[tokio::test]
    async fn test_invalidation_burst_flushed_by_next_completed_batch() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let manager = new_manager(vault.path()).await;
        manager.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(read_envelope(vault.path()).entries.len(), 3);

        // Watcher-style burst: several invalidations, no disk writes between.
        manager.invalidate("knowledge/rust.md").await;
        manager.invalidate("knowledge/gardening.md").await;

        // A search touching only the remaining note (all cache hits) flushes.
        let results = manager
            .get_embeddings_batch(&[note("journal/2026-09-04.md", &notes[2].1)])
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(manager.encoded_text_count(), 3, "no re-encoding, hits only");

        let envelope = read_envelope(vault.path());
        assert_eq!(envelope.entries.len(), 1);
        assert!(envelope.entries.contains_key("journal/2026-09-04.md"));
    }

    /// A different model fingerprint invalidates the whole snapshot: rebuild,
    /// and the next publish overwrites the file with the correct identity.
    #[tokio::test]
    async fn test_model_fingerprint_mismatch_rebuilds() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let first = new_manager(vault.path()).await;
        first.get_embeddings_batch(&notes).await.unwrap();
        drop(first);

        // Tamper: same shape, different model identity.
        let mut envelope = read_envelope(vault.path());
        envelope.model_fingerprint = "all-MiniLM-L6-v2-but-actually-different".to_string();
        write_envelope(vault.path(), &envelope);

        let second = new_manager(vault.path()).await;
        let results = second.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(
            second.encoded_text_count(),
            3,
            "incompatible model identity must rebuild, not mix vectors"
        );
        assert_eq!(results.len(), 3);

        let republished = read_envelope(vault.path());
        assert_ne!(
            republished.model_fingerprint,
            "all-MiniLM-L6-v2-but-actually-different"
        );
        assert_eq!(republished.entries.len(), 3);
    }

    /// Missing, malformed and truncated caches all rebuild safely and leave a
    /// valid populated snapshot behind.
    #[tokio::test]
    async fn test_missing_malformed_and_truncated_caches_rebuild() {
        let notes = demo_notes();

        // Missing.
        let vault = tempfile::tempdir().unwrap();
        let manager = new_manager(vault.path()).await;
        let results = manager.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(read_envelope(vault.path()).entries.len(), 3);
        drop(manager);

        // Malformed.
        let vault = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(vault.path().join(".obsidian")).unwrap();
        std::fs::write(
            vault.path().join(".obsidian").join(CACHE_FILENAME),
            b"{corrupt!!",
        )
        .unwrap();
        let manager = new_manager(vault.path()).await;
        let results = manager.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(read_envelope(vault.path()).entries.len(), 3);
        drop(manager);

        // Truncated (simulated interrupted write).
        let vault = tempfile::tempdir().unwrap();
        let manager = new_manager(vault.path()).await;
        manager.get_embeddings_batch(&notes).await.unwrap();
        drop(manager);
        let cache_path = vault.path().join(".obsidian").join(CACHE_FILENAME);
        let json = std::fs::read_to_string(&cache_path).unwrap();
        std::fs::write(&cache_path, &json[..json.len() / 2]).unwrap();

        let manager = new_manager(vault.path()).await;
        let results = manager.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(read_envelope(vault.path()).entries.len(), 3);
    }

    /// Malformed envelope records (wrong dimension) are dropped at load, not
    /// trusted, while all well-formed records — including legal note names
    /// beginning with `__` — are reused.
    #[tokio::test]
    async fn test_malformed_records_are_dropped_not_reused() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let manager = new_manager(vault.path()).await;
        manager.get_embeddings_batch(&notes).await.unwrap();
        drop(manager);

        let mut envelope = read_envelope(vault.path());
        envelope.entries.insert(
            "notes/wrongdim.md".to_string(),
            serde_json::from_value(serde_json::json!({
                "content_hash": "h",
                "embedding": [0.1, 0.2]
            }))
            .unwrap(),
        );
        // NOTE: non-finite vectors cannot be represented in JSON at all (they
        // arrive only from in-memory corruption, covered by the persist unit
        // tests), so the file-level fixture uses shape malformations.
        envelope.entries.insert(
            "__valid-note.md".to_string(),
            envelope.entries["knowledge/rust.md"].clone(),
        );
        write_envelope(vault.path(), &envelope);

        let second = new_manager(vault.path()).await;
        let results = second.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(
            second.encoded_text_count(),
            0,
            "well-formed entries are still reused"
        );
        assert_eq!(
            results.len(),
            3,
            "malformed records never surface as results"
        );

        let republished = read_envelope(vault.path());
        assert_eq!(republished.entries.len(), 4);
        assert!(!republished.entries.contains_key("notes/wrongdim.md"));
        assert!(republished.entries.contains_key("__valid-note.md"));
    }

    /// Regression for the parent's real MCP probe: a note named
    /// `__valid-note.md` is a legal note name. After a restart it must be
    /// reused from the cache without re-encoding, not re-encoded on every
    /// process launch.
    #[tokio::test]
    async fn test_dunder_prefixed_note_is_reused_after_restart() {
        let vault = tempfile::tempdir().unwrap();
        let dunder_note = note(
            "__valid-note.md",
            "A perfectly ordinary note with a double-underscore name.",
        );

        let first = new_manager(vault.path()).await;
        let first_results = first
            .get_embeddings_batch(std::slice::from_ref(&dunder_note))
            .await
            .unwrap();
        assert_eq!(first.encoded_text_count(), 1);
        assert!(
            read_envelope(vault.path())
                .entries
                .contains_key("__valid-note.md")
        );
        drop(first);

        let second = new_manager(vault.path()).await;
        let second_results = second
            .get_embeddings_batch(std::slice::from_ref(&dunder_note))
            .await
            .unwrap();
        assert_eq!(
            second.encoded_text_count(),
            0,
            "a __-prefixed legal note name must be reused after restart, not re-encoded"
        );
        assert_eq!(first_results, second_results);
    }

    /// Regression for the parent's zero-vector probe: a cached vector of 384
    /// zeroes (and, per cosine similarity's actual arithmetic, finite vectors
    /// whose norm overflows to +inf or underflows to 0) must be recomputed on
    /// restart instead of being reused to silently erase search matches.
    #[tokio::test]
    async fn test_zero_and_degenerate_norm_vectors_are_recomputed() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let first = new_manager(vault.path()).await;
        first.get_embeddings_batch(&notes).await.unwrap();
        drop(first);

        // Tamper three otherwise-compatible entries with finite vectors whose
        // similarity arithmetic is degenerate.
        let mut envelope = read_envelope(vault.path());
        envelope
            .entries
            .get_mut("knowledge/rust.md")
            .unwrap()
            .embedding = vec![0.0; EMBEDDING_DIM];
        envelope
            .entries
            .get_mut("knowledge/gardening.md")
            .unwrap()
            .embedding = vec![1e20; EMBEDDING_DIM];
        envelope
            .entries
            .get_mut("journal/2026-09-04.md")
            .unwrap()
            .embedding = vec![1e-25; EMBEDDING_DIM];
        write_envelope(vault.path(), &envelope);

        let second = new_manager(vault.path()).await;
        let results = second.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(
            second.encoded_text_count(),
            3,
            "zero/overflowing/underflowing-norm vectors must be recomputed"
        );
        assert_eq!(results.len(), 3);
        for (_, embedding) in &results {
            let norm = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(
                norm.is_finite() && norm > 0.0,
                "recomputed vector must have a usable norm"
            );
        }

        // The republished snapshot no longer holds the degenerate vectors.
        let republished = read_envelope(vault.path());
        assert_ne!(
            republished.entries["knowledge/rust.md"].embedding,
            vec![0.0; EMBEDDING_DIM]
        );
    }

    /// Deterministic interleaving regression at the production flush seam:
    /// a mutation that lands between the snapshot file write and the commit
    /// must keep the cache unpersisted, so the next completed batch — even an
    /// all-hits one — flushes it.
    #[tokio::test]
    async fn test_flush_racing_mutation_keeps_unpersisted_state() {
        let vault = tempfile::tempdir().unwrap();
        let notes = demo_notes();

        let manager = EmbeddingManager::new(vault.path());
        manager.initialize().await.expect("model + cache init");
        manager.get_embeddings_batch(&notes).await.unwrap();
        assert_eq!(read_envelope(vault.path()).entries.len(), 3);

        // Arm the flush gate, then run the production flush in a task.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *manager.flush_gate.lock().unwrap() = Some((reached_tx, release_rx));
        let manager = Arc::new(manager);

        let flush_mgr = Arc::clone(&manager);
        let flush_task = tokio::spawn(async move { flush_mgr.flush_snapshot().await });

        // The flush is now paused after writing the file, before committing
        // the captured generation. Inject a mutation into exactly that window.
        reached_rx
            .await
            .expect("flush must reach the commit boundary");
        manager.invalidate("knowledge/rust.md").await;
        release_tx
            .send(())
            .expect("flush still waiting on the gate");
        flush_task.await.unwrap().expect("flush must succeed");

        // A commit must acknowledge only its captured generation.
        assert!(
            manager.has_unpersisted_mutations(),
            "a mutation racing the snapshot write must keep the cache unpersisted"
        );

        // The next completed batch — over the surviving notes, all cache
        // hits — must flush the raced mutation and drop the invalidated note.
        let surviving = notes[1..].to_vec();
        let results = manager.get_embeddings_batch(&surviving).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            manager.encoded_text_count(),
            3,
            "all hits, no re-encoding; the flush must not depend on it"
        );
        assert!(!manager.has_unpersisted_mutations());

        let envelope = read_envelope(vault.path());
        assert_eq!(envelope.entries.len(), 2);
        assert!(!envelope.entries.contains_key("knowledge/rust.md"));
    }

    /// The startup preload and a concurrent on-demand search interleave chunk
    /// inserts; each published snapshot is a consistent point-in-time clone
    /// and the completed preload leaves the full populated cache persisted.
    /// No global barrier: the search must not wait for the preload.
    #[tokio::test]
    async fn test_preload_and_search_interleaving_publishes_consistent_snapshot() {
        let vault = tempfile::tempdir().unwrap();

        // Enough notes to span multiple 25-note chunks so chunk boundaries can
        // actually interleave.
        let preload_notes: Vec<(String, String)> = (0..60)
            .map(|i| {
                note(
                    &format!("notes/n{i}.md"),
                    &format!("Note {i} about caching, gardens, and systems programming."),
                )
            })
            .collect();
        let search_notes: Vec<(String, String)> = preload_notes[..10].to_vec();

        let manager = new_manager(vault.path()).await;

        let preload_mgr = manager.clone();
        let preload =
            tokio::spawn(async move { preload_mgr.get_embeddings_batch(&preload_notes).await });
        let search_mgr = manager.clone();
        let search =
            tokio::spawn(async move { search_mgr.get_embeddings_batch(&search_notes).await });

        let preload_results = preload.await.unwrap().unwrap();
        let search_results = search.await.unwrap().unwrap();
        assert_eq!(preload_results.len(), 60);
        assert_eq!(search_results.len(), 10);

        let envelope = read_envelope(vault.path());
        assert_eq!(envelope.format_version, CACHE_FORMAT_VERSION);
        assert_eq!(envelope.embedding_version, EMBEDDING_PIPELINE_VERSION);
        assert_eq!(envelope.dimension, EMBEDDING_DIM);
        assert!(!envelope.model_fingerprint.is_empty());
        assert_eq!(
            envelope.entries.len(),
            60,
            "completed preload fully persisted"
        );
        for (path, entry) in &envelope.entries {
            assert_eq!(entry.embedding.len(), EMBEDDING_DIM, "consistent {path}");
            assert!(
                entry.embedding.iter().all(|v| v.is_finite()),
                "finite {path}"
            );
        }
    }

    /// Query encoding is ephemeral: it never creates or dirties the note cache.
    #[tokio::test]
    async fn test_query_encoding_is_not_persisted() {
        let vault = tempfile::tempdir().unwrap();
        let manager = new_manager(vault.path()).await;

        let embeddings = manager
            .encode_query_texts(&["how do I keep tomatoes alive?".to_string()])
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), EMBEDDING_DIM);

        assert_eq!(manager.encoded_text_count(), 1);
        assert!(!vault.path().join(".obsidian").join(CACHE_FILENAME).exists());
    }
}
