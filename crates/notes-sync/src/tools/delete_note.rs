//! DeleteNote tool - delete a note from the vault.

use obsidian_fs::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use std::path::Path;
use tokio::sync::RwLock;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::storage::{Storage, StorageError};

/// Execute the DeleteNote tool.
///
/// Permanently deletes a note from the vault.
/// Uses graph-aware resolution so plain names find notes in subdirectories.
pub async fn execute<S: Storage>(
    vault_path: &Path,
    storage: &S,
    graph: &RwLock<GraphIndex>,
    note: &str,
) -> Result<CallToolResult, ErrorData> {
    let (uri, exists) = {
        let graph_read = graph.read().await;
        resolve_note_uri(storage, &graph_read, note)
            .await
            .map_err(|e| {
                ErrorData::internal_error(format!("Failed to resolve note: {}", e), None)
            })?
    };

    if !exists {
        let file_path = vault_path
            .join(ensure_markdown_extension(&uri))
            .to_string_lossy()
            .to_string();
        return Err(ErrorData::invalid_params(
            format!(
                "Note not found: {}. Cannot delete a note that doesn't exist.\n\
                 Expected file: {}",
                note, file_path
            ),
            None,
        ));
    }

    let file_path = vault_path
        .join(ensure_markdown_extension(&uri))
        .to_string_lossy()
        .to_string();

    // Delete the note
    storage.delete(&uri).await.map_err(|e| match e {
        StorageError::NotFound { uri } => ErrorData::invalid_params(
            format!(
                "Note not found: {}. Cannot delete a note that doesn't exist.\n\
                 Expected file: {}",
                uri, file_path
            ),
            None,
        ),
        _ => ErrorData::internal_error(format!("Failed to delete note: {}", e), None),
    })?;

    // Extract display name from the URI (last path component)
    let display_name = uri.rsplit('/').next().unwrap_or(&uri);

    let text = format!(
        "Deleted note: {}\n\n\
         **URI:** memory:{}\n\
         **File:** {}\n\n\
         The note has been permanently deleted.",
        display_name, uri, file_path
    );

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphIndex;
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
    async fn test_delete_existing_note() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Content")
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "test")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Deleted note"));
        assert!(text.contains("memory:test"));

        // Verify file was deleted
        assert!(!temp_dir.path().join("test.md").exists());
    }

    #[tokio::test]
    async fn test_delete_note_in_subdirectory() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("knowledge/test.md"), "Content")
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "knowledge/test")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Deleted note"));
        assert!(!temp_dir.path().join("knowledge/test.md").exists());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_note() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute(temp_dir.path(), &storage, &graph, "nonexistent").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note not found"));
    }

    #[tokio::test]
    async fn test_delete_with_wiki_link_syntax() {
        let (temp_dir, storage, graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Content")
            .await
            .unwrap();

        let result = execute(temp_dir.path(), &storage, &graph, "[[test]]")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Deleted note"));
        assert!(!temp_dir.path().join("test.md").exists());
    }

    #[tokio::test]
    async fn test_delete_plain_name_resolves_subdirectory() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Create note in subdirectory and register in graph
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("knowledge/My Note.md"), "Content")
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

        // Delete using just the plain name — should resolve via graph
        let result = execute(temp_dir.path(), &storage, &graph, "My Note")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Deleted note"));
        assert!(!temp_dir.path().join("knowledge/My Note.md").exists());
    }
}
