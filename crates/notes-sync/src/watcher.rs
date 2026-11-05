//! File watcher for keeping the graph index and embeddings up to date.
//!
//! Watches the vault directory for changes to markdown files and updates
//! the graph index and invalidates stale embeddings.
//! Uses debouncing to batch rapid changes.
//!
//! Note: The read whitelist is NOT invalidated by the file watcher. Instead,
//! staleness is detected at write-time using content hash comparison. This
//! approach correctly handles:
//! - Multiple MCP servers (each checks against actual file content)
//! - Self-edits (client's own writes don't invalidate their whitelist entry)
//! - External edits (hash mismatch triggers re-read requirement)

use notify::RecommendedWatcher;
use notify_debouncer_mini::{DebouncedEventKind, Debouncer, new_debouncer};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, mpsc};
use wiki_links::extract_linked_notes;

use crate::embeddings::EmbeddingManager;
use crate::graph::GraphIndex;

/// Watches vault directory and updates graph index on file changes.
pub struct VaultWatcher {
    _debouncer: Debouncer<RecommendedWatcher>,
}

impl VaultWatcher {
    /// Start watching the vault directory.
    ///
    /// Spawns a background task that listens for file system events and
    /// updates the graph index and invalidates embeddings when markdown files change.
    pub fn start(
        vault_path: PathBuf,
        graph: Arc<RwLock<GraphIndex>>,
        embeddings: Arc<EmbeddingManager>,
    ) -> Result<Self, notify::Error> {
        let (tx, rx) = mpsc::channel(100);
        let vault_path_clone = vault_path.clone();

        // Create debouncer that sends events to our channel
        let mut debouncer = new_debouncer(
            Duration::from_millis(500),
            move |result: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
                if let Ok(events) = result {
                    // Filter for markdown files only
                    let md_events: Vec<_> = events
                        .into_iter()
                        .filter(|e| e.path.extension().map(|ext| ext == "md").unwrap_or(false))
                        .collect();

                    if !md_events.is_empty() {
                        let _ = tx.blocking_send(md_events);
                    }
                }
            },
        )?;

        // Start watching the vault
        debouncer
            .watcher()
            .watch(&vault_path, notify::RecursiveMode::Recursive)?;

        tracing::info!("Started file watcher for {}", vault_path.display());

        // Spawn background task to process events
        tokio::spawn(process_events(rx, vault_path_clone, graph, embeddings));

        Ok(Self {
            _debouncer: debouncer,
        })
    }
}

/// Process file system events and update the graph index and embeddings.
///
/// Uses mtime tracking to avoid infinite loops on Docker bind mounts where
/// reading files can trigger additional filesystem events (atime updates).
async fn process_events(
    mut rx: mpsc::Receiver<Vec<notify_debouncer_mini::DebouncedEvent>>,
    vault_path: PathBuf,
    graph: Arc<RwLock<GraphIndex>>,
    embeddings: Arc<EmbeddingManager>,
) {
    // Track file mtimes to detect real changes vs spurious events
    let mut mtime_cache: HashMap<PathBuf, SystemTime> = HashMap::new();

    while let Some(events) = rx.recv().await {
        for event in events {
            let path = &event.path;

            // Skip hidden files and directories
            if path
                .components()
                .any(|c| c.as_os_str().to_string_lossy().starts_with('.'))
            {
                continue;
            }

            match event.kind {
                DebouncedEventKind::Any => {
                    if path.exists() {
                        // Check mtime BEFORE processing to avoid infinite loops
                        let current_mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();

                        let cached_mtime = mtime_cache.get(path).copied();

                        if current_mtime.is_some() && current_mtime == cached_mtime {
                            tracing::trace!("Skipping {} - mtime unchanged", path.display());
                            continue;
                        }

                        // Cache mtime BEFORE reading file (prevents race condition)
                        if let Some(mtime) = current_mtime {
                            mtime_cache.insert(path.clone(), mtime);
                        }

                        // Get relative path for embedding cache key
                        let relative_path = path.strip_prefix(&vault_path).unwrap_or(path);
                        let relative_path_str = relative_path.to_string_lossy().to_string();

                        // Now process the file
                        tracing::trace!("Processing {} - mtime changed", path.display());
                        if let Err(e) = update_file(&vault_path, path, &graph).await {
                            tracing::warn!("Failed to update index for {}: {}", path.display(), e);
                        }
                        // Invalidate stale embedding so it gets recomputed on next search
                        embeddings.invalidate(&relative_path_str).await;
                        tracing::debug!("Invalidated embedding for: {}", relative_path_str);
                    } else {
                        // File was deleted - clean up cache entry
                        mtime_cache.remove(path);

                        let relative_path = path.strip_prefix(&vault_path).unwrap_or(path);
                        let relative_path_str = relative_path.to_string_lossy().to_string();

                        remove_file(&vault_path, path, &graph).await;
                        embeddings.invalidate(&relative_path_str).await;
                    }
                    // Note: Read whitelist is NOT invalidated here. Staleness is detected
                    // at write-time via content hash comparison. This handles self-edits
                    // correctly (client's own writes don't block subsequent writes).
                }
                DebouncedEventKind::AnyContinuous => {
                    // Continuous events (like ongoing writes) - skip until settled
                }
                _ => {
                    // Handle any future event kinds
                }
            }
        }
    }
}

/// Update the graph index for a single file.
async fn update_file(
    vault_path: &Path,
    file_path: &Path,
    graph: &Arc<RwLock<GraphIndex>>,
) -> Result<(), std::io::Error> {
    let content = tokio::fs::read_to_string(file_path).await?;

    // Get note name (filename without .md extension)
    let note_name = file_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();

    // Get relative path from vault root
    let relative_path = file_path
        .strip_prefix(vault_path)
        .unwrap_or(file_path)
        .to_path_buf();

    // Extract linked notes
    let linked_notes = extract_linked_notes(&content);
    let links: HashSet<String> = linked_notes.into_iter().collect();

    // Update the graph
    let mut graph = graph.write().await;
    graph.update_note(&note_name, relative_path.clone(), links);

    tracing::debug!("Updated index for: {}", relative_path.display());

    Ok(())
}

/// Remove a file from the graph index.
async fn remove_file(vault_path: &Path, file_path: &Path, graph: &Arc<RwLock<GraphIndex>>) {
    let note_name = file_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();

    let relative_path = file_path.strip_prefix(vault_path).unwrap_or(file_path);

    if !note_name.is_empty() {
        let mut graph = graph.write().await;
        graph.remove_note(&note_name, relative_path);
        tracing::debug!("Removed from index: {}", relative_path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_watcher_starts_successfully() {
        let temp_dir = TempDir::new().unwrap();
        let graph = Arc::new(RwLock::new(GraphIndex::new()));
        let embeddings = Arc::new(EmbeddingManager::new(temp_dir.path()));

        let watcher = VaultWatcher::start(temp_dir.path().to_path_buf(), graph, embeddings);
        assert!(watcher.is_ok());
    }

    #[tokio::test]
    async fn test_update_file_indexes_links() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.md");
        fs::write(&file_path, "Links to [[Note A]] and [[Note B]]").unwrap();

        let graph = Arc::new(RwLock::new(GraphIndex::new()));

        update_file(temp_dir.path(), &file_path, &graph)
            .await
            .unwrap();

        let graph = graph.read().await;
        // get_forward_links now takes a path
        let links = graph.get_forward_links("test.md").unwrap();
        assert!(links.contains("Note A"));
        assert!(links.contains("Note B"));
    }

    #[tokio::test]
    async fn test_remove_file_clears_index() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.md");

        let graph = Arc::new(RwLock::new(GraphIndex::new()));

        // First add the note
        {
            let mut g = graph.write().await;
            g.update_note(
                "test",
                PathBuf::from("test.md"),
                ["Note A"].iter().map(|s| s.to_string()).collect(),
            );
        }

        // Verify it exists
        {
            let g = graph.read().await;
            assert!(g.get_forward_links("test.md").is_some());
        }

        // Remove it - now takes vault_path as well
        remove_file(temp_dir.path(), &file_path, &graph).await;

        // Verify it's gone
        let g = graph.read().await;
        assert!(g.get_forward_links("test.md").is_none());
    }
}
