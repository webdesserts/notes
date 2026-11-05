//! MoveNote tool - move/rename a note and update backlinks.

use notes_core::note_ref::{ensure_markdown_extension, normalize_note_reference};
use rmcp::model::{CallToolResult, Content, ErrorData};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::storage::{Storage, StorageError};
use notes_core::wiki_links::extract_linked_notes;

/// Execute the MoveNote tool.
///
/// Moves or renames a note and updates all notes that link to it.
/// The `from` parameter uses graph-aware resolution so plain names find notes
/// in subdirectories. The `to` parameter is a literal destination path.
pub async fn execute<S: Storage>(
    vault_path: &Path,
    storage: &S,
    graph: &RwLock<GraphIndex>,
    from: &str,
    to: &str,
) -> Result<CallToolResult, ErrorData> {
    // Resolve source through graph (handles plain names in subdirectories)
    let (from_uri, from_exists) = {
        let graph_read = graph.read().await;
        resolve_note_uri(storage, &graph_read, from)
            .await
            .map_err(|e| {
                ErrorData::internal_error(format!("Failed to resolve source: {}", e), None)
            })?
    };

    // Destination is a literal path, not a graph lookup
    let to_normalized = normalize_note_reference(to);
    let to_uri = &to_normalized.path;

    let from_file = vault_path
        .join(ensure_markdown_extension(&from_uri))
        .to_string_lossy()
        .to_string();
    let to_file = vault_path
        .join(ensure_markdown_extension(to_uri))
        .to_string_lossy()
        .to_string();

    if !from_exists {
        return Err(ErrorData::invalid_params(
            format!(
                "Source note not found: {}. Cannot move a note that doesn't exist.\n\
                 Expected file: {}",
                from, from_file
            ),
            None,
        ));
    }

    // Check destination doesn't exist
    if storage.exists(to_uri).await.map_err(|e| {
        ErrorData::internal_error(format!("Failed to check destination: {}", e), None)
    })? {
        return Err(ErrorData::invalid_params(
            format!(
                "Destination already exists: {}. Cannot overwrite an existing note.\n\
                 Existing file: {}",
                to, to_file
            ),
            None,
        ));
    }

    // Extract display names from URIs (last path component)
    let from_name = from_uri.rsplit('/').next().unwrap_or(&from_uri).to_string();
    let from_path = ensure_markdown_extension(&from_uri);

    // Notes that link to the source by name. Path-qualified links
    // (`[[folder/Name]]`) are indexed under the bare name too.
    let mut linking_paths: Vec<String> = {
        let graph_read = graph.read().await;
        graph_read
            .get_backlinks(&from_name)
            .map(|paths| paths.iter().cloned().collect())
            .unwrap_or_default()
    };
    linking_paths.sort();

    // Rewrite backlinks before the move
    let mut backlinks_updated = Vec::new();
    let mut rewritten: Vec<(String, String)> = Vec::new();
    for path in &linking_paths {
        // Skip the source note itself
        if path == &from_path {
            continue;
        }

        // Convert path to URI (remove .md)
        let uri = path.strip_suffix(".md").unwrap_or(path);

        if let Ok((content, _)) = storage.read(uri).await {
            let updated =
                rewrite_wiki_links(&content, &from_name, &from_uri, &to_normalized.name, to_uri);
            if updated != content && storage.write(uri, &updated, None).await.is_ok() {
                backlinks_updated.push(uri.to_string());
                rewritten.push((path.clone(), updated));
            }
        }
    }

    // Perform the rename
    storage
        .rename(&from_uri, to_uri)
        .await
        .map_err(|e| match e {
            StorageError::ParentNotFound { uri, parent } => ErrorData::invalid_params(
                format!(
                    "Parent directory doesn't exist for '{}': {}. \
                 Create the directory first.",
                    uri,
                    parent.display()
                ),
                None,
            ),
            _ => ErrorData::internal_error(format!("Failed to move note: {}", e), None),
        })?;

    // Reflect the move in the graph right away instead of waiting for the
    // watcher: the moved note under its new path, and the rewritten notes'
    // new forward links.
    {
        let read_links: Option<HashSet<String>> = storage
            .read(to_uri)
            .await
            .ok()
            .map(|(content, _)| extract_linked_notes(&content).into_iter().collect());
        let mut g = graph.write().await;
        let moved_links = read_links
            .or_else(|| g.get_forward_links(&from_path).cloned())
            .unwrap_or_default();
        g.remove_note(&from_name, Path::new(&from_path));
        g.update_note(
            &to_normalized.name,
            PathBuf::from(ensure_markdown_extension(to_uri)),
            moved_links,
        );
        for (path, content) in &rewritten {
            let name = Path::new(path)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            g.update_note(
                &name,
                PathBuf::from(path),
                extract_linked_notes(content).into_iter().collect(),
            );
        }
    }

    // Build response
    let backlinks_summary = if !backlinks_updated.is_empty() {
        format!(
            "\n\n## Updated Backlinks\n\nUpdated {} note(s) that linked to this note:\n{}",
            backlinks_updated.len(),
            backlinks_updated
                .iter()
                .map(|p| format!("- memory:{}", p))
                .collect::<Vec<_>>()
                .join("\n")
        )
    } else {
        "\n\nNo backlinks to update.".to_string()
    };

    let text = format!(
        "Moved note: {} -> {}\n\n\
         **From:** memory:{}\n\
         **To:** memory:{}\n\
         **New file:** {}{}",
        from_name, to_normalized.name, from_uri, to_uri, to_file, backlinks_summary
    );

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

/// Rewrite wiki-links that point at a moved note.
///
/// Handles plain (`[[Name]]`), path-qualified (`[[folder/Name]]`), aliased
/// (`[[Name|alias]]`), heading (`[[Name#h]]`) and embed (`![[Name]]`) forms.
/// Path-qualified links are rewritten only when the source lived in a folder;
/// plain links are rewritten only when the note's name changes, so a move
/// between folders that keeps the name leaves `[[Name]]` alone.
fn rewrite_wiki_links(
    content: &str,
    from_name: &str,
    from_uri: &str,
    to_name: &str,
    to_uri: &str,
) -> String {
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    if from_uri.contains('/') && from_uri != to_uri {
        pairs.push((from_uri, to_uri));
    }
    if from_name != to_name {
        pairs.push((from_name, to_name));
    }
    let mut out = content.to_string();
    for (old, new) in pairs {
        for suffix in ["]]", "|", "#"] {
            out = out.replace(&format!("[[{old}{suffix}"), &format!("[[{new}{suffix}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FileStorage;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs;

    async fn create_test_env() -> (TempDir, FileStorage, Arc<RwLock<GraphIndex>>) {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path().to_path_buf());
        let graph = Arc::new(RwLock::new(GraphIndex::new()));
        (temp_dir, storage, graph)
    }

    #[tokio::test]
    async fn test_move_simple_rename() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("old.md"), "Content")
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "old", "new")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Moved note"));
        assert!(!temp_dir.path().join("old.md").exists());
        assert!(temp_dir.path().join("new.md").exists());
    }

    #[tokio::test]
    async fn test_move_between_directories() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Content")
            .await
            .unwrap();
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "test", "knowledge/test")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Moved note"));
        assert!(!temp_dir.path().join("test.md").exists());
        assert!(temp_dir.path().join("knowledge/test.md").exists());
    }

    #[tokio::test]
    async fn test_move_updates_backlinks() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Create note A that links to B
        fs::write(temp_dir.path().join("A.md"), "Link to [[B]]")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("B.md"), "Target note")
            .await
            .unwrap();

        // Update graph with the link
        {
            let mut g = graph.write().await;
            g.update_note(
                "A",
                PathBuf::from("A.md"),
                ["B".to_string()].into_iter().collect(),
            );
            g.update_note("B", PathBuf::from("B.md"), HashSet::new());
        }

        let result = execute(temp_dir.path(), &storage, &graph, "B", "C")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Updated Backlinks"));
        assert!(text.contains("memory:A"));

        // Verify A was updated
        let a_content = fs::read_to_string(temp_dir.path().join("A.md"))
            .await
            .unwrap();
        assert!(a_content.contains("[[C]]"));
        assert!(!a_content.contains("[[B]]"));
    }

    #[tokio::test]
    async fn test_move_source_not_found() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute(temp_dir.path(), &storage, &graph, "nonexistent", "new").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Source note not found"));
    }

    #[tokio::test]
    async fn test_move_destination_exists() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("source.md"), "Source")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("dest.md"), "Dest")
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "source", "dest").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Destination already exists"));
    }

    #[tokio::test]
    async fn test_move_parent_missing() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Content")
            .await
            .unwrap();

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "missing/dir/test",
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Parent directory doesn't exist"));
    }

    #[tokio::test]
    async fn test_move_plain_name_resolves_subdirectory() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Create note in subdirectory and register in graph
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(
            temp_dir.path().join("knowledge/My Note.md"),
            "Original content",
        )
        .await
        .unwrap();
        {
            let mut g = graph.write().await;
            g.update_note(
                "My Note",
                PathBuf::from("knowledge/My Note.md"),
                HashSet::new(),
            );
        }

        // Move using just the plain name — should resolve via graph
        let result = execute(temp_dir.path(), &storage, &graph, "My Note", "Renamed Note")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Moved note"));
        assert!(!temp_dir.path().join("knowledge/My Note.md").exists());
        assert!(temp_dir.path().join("Renamed Note.md").exists());
    }

    async fn index(temp: &TempDir, graph: &Arc<RwLock<GraphIndex>>, rel: &str, content: &str) {
        let full = temp.path().join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(&full, content).await.unwrap();
        let name = Path::new(rel)
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();
        graph.write().await.update_note(
            &name,
            PathBuf::from(rel),
            extract_linked_notes(content).into_iter().collect(),
        );
    }

    #[tokio::test]
    async fn test_move_between_directories_updates_path_qualified_links() {
        let (temp_dir, storage, graph) = create_test_env().await;
        index(&temp_dir, &graph, "architecture/Foo.md", "Foo body").await;
        index(
            &temp_dir,
            &graph,
            "Linker.md",
            "See [[architecture/Foo]] and [[architecture/Foo|Foo doc]] and [[Foo]].",
        )
        .await;
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();

        execute(
            temp_dir.path(),
            &storage,
            &graph,
            "architecture/Foo",
            "knowledge/Foo",
        )
        .await
        .expect("should succeed");

        let linker = fs::read_to_string(temp_dir.path().join("Linker.md"))
            .await
            .unwrap();
        assert_eq!(
            linker,
            "See [[knowledge/Foo]] and [[knowledge/Foo|Foo doc]] and [[Foo]]."
        );
        assert!(temp_dir.path().join("knowledge/Foo.md").exists());
    }

    #[tokio::test]
    async fn test_move_updates_aliased_backlinks() {
        let (temp_dir, storage, graph) = create_test_env().await;
        index(&temp_dir, &graph, "Old.md", "Old body").await;
        index(
            &temp_dir,
            &graph,
            "A.md",
            "[[Old|my alias]], [[Old#Heading]], ![[Old]], [[Older]]",
        )
        .await;

        execute(temp_dir.path(), &storage, &graph, "Old", "New")
            .await
            .expect("should succeed");

        let a = fs::read_to_string(temp_dir.path().join("A.md"))
            .await
            .unwrap();
        assert_eq!(a, "[[New|my alias]], [[New#Heading]], ![[New]], [[Older]]");
    }

    #[tokio::test]
    async fn test_move_updates_graph_immediately() {
        let (temp_dir, storage, graph) = create_test_env().await;
        index(&temp_dir, &graph, "Old.md", "Links to [[Other]]").await;
        index(&temp_dir, &graph, "A.md", "Link to [[Old]]").await;

        execute(temp_dir.path(), &storage, &graph, "Old", "New")
            .await
            .expect("should succeed");

        let g = graph.read().await;
        assert!(g.get_forward_links("Old.md").is_none());
        let moved = g.get_forward_links("New.md").expect("moved note indexed");
        assert!(moved.contains("Other"));
        let back = g.get_backlinks("New").expect("backlinks to new name");
        assert!(back.contains("A.md"));
        assert!(g.get_backlinks("Old").is_none_or(|b| b.is_empty()));
    }

    #[test]
    fn test_rewrite_keeps_plain_links_when_name_unchanged() {
        let out = rewrite_wiki_links(
            "[[Foo]] [[a/Foo]] [[a/Foobar]]",
            "Foo",
            "a/Foo",
            "Foo",
            "b/Foo",
        );
        assert_eq!(out, "[[Foo]] [[b/Foo]] [[a/Foobar]]");
    }
}
