use obsidian_fs::{
    Frontmatter, build_note_with_frontmatter, ensure_markdown_extension, parse_frontmatter,
};
use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::storage::{ContentHash, Storage, StorageError};

/// Response from UpdateFrontmatter tool.
#[derive(Serialize)]
pub struct UpdateFrontmatterResponse {
    /// The file path relative to vault
    pub path: String,
    /// New content hash after update - use this for subsequent writes
    pub content_hash: String,
}

/// Update frontmatter in a note file.
///
/// Reads the existing note, merges the frontmatter updates, and writes back.
/// Requires content_hash from a previous ReadNote call.
pub async fn execute<S: Storage>(
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    updates: HashMap<String, JsonValue>,
    content_hash: &str,
) -> Result<CallToolResult, ErrorData> {
    // Resolve the note reference using the graph index
    let (uri, exists) = resolve_note_uri(storage, graph, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    if !exists {
        return Err(ErrorData::invalid_params(
            format!("Note not found: {}", note),
            None,
        ));
    }

    // Read existing content via Storage trait
    let (raw_content, _metadata) = storage.read(&uri).await.map_err(|e| match e {
        StorageError::NotFound { uri } => {
            ErrorData::invalid_params(format!("Note not found: {}", uri), None)
        }
        _ => ErrorData::internal_error(format!("Failed to read note: {}", e), None),
    })?;

    // Validate content_hash matches current content
    let current_hash = ContentHash::from_content(&raw_content);
    if current_hash.as_str() != content_hash {
        return Err(ErrorData::invalid_params(
            "Note modified since last read. Read the note again to get the \
             current content and hash before retrying."
                .to_string(),
            None,
        ));
    }

    // Parse existing frontmatter
    let parsed = parse_frontmatter(&raw_content);
    let existing_frontmatter = parsed.frontmatter.unwrap_or_default();
    let content = parsed.content;

    // Merge updates into existing frontmatter
    let mut merged: Frontmatter = existing_frontmatter;
    for (key, value) in updates {
        merged.insert(key, value);
    }

    // Rebuild file content with updated frontmatter
    let new_content = build_note_with_frontmatter(&merged, content)
        .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;

    // Write back via Storage trait with optimistic locking
    storage
        .write(&uri, &new_content, Some(content_hash))
        .await
        .map_err(|e| match e {
            StorageError::HashMismatch { .. } => ErrorData::invalid_params(
                "Note modified since last read. Read the note again to get the \
                 current content and hash before retrying."
                    .to_string(),
                None,
            ),
            _ => ErrorData::internal_error(format!("Failed to write note: {}", e), None),
        })?;

    // Compute new content hash for response
    let new_hash = ContentHash::from_content(&new_content);

    let response = UpdateFrontmatterResponse {
        path: ensure_markdown_extension(&uri),
        content_hash: new_hash.as_str().to_string(),
    };

    let json = serde_json::to_string(&response).map_err(|e| {
        ErrorData::internal_error(format!("Failed to serialize response: {}", e), None)
    })?;

    Ok(CallToolResult::success(vec![Content::text(json)]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FileStorage;
    use serde::Deserialize;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    #[derive(Deserialize)]
    struct TestResponse {
        path: String,
        content_hash: String,
    }

    async fn create_test_env() -> (TempDir, FileStorage, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path().to_path_buf());
        let graph = GraphIndex::new();
        (temp_dir, storage, graph)
    }

    async fn create_test_note(dir: &std::path::Path, name: &str, content: &str) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(path, content).await.unwrap();
    }

    fn parse_response(result: &CallToolResult) -> TestResponse {
        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();
        serde_json::from_str(&text).expect("Expected valid JSON")
    }

    #[tokio::test]
    async fn test_update_existing_frontmatter() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let initial_content = "---\ntype: note\ntags:\n  - one\n---\n\nContent here";
        create_test_note(temp_dir.path(), "test.md", initial_content).await;
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(initial_content);

        let mut updates = HashMap::new();
        updates.insert("type".to_string(), JsonValue::String("updated".to_string()));
        updates.insert("new_field".to_string(), JsonValue::Bool(true));

        let result = execute(&storage, &graph, "test", updates, content_hash.as_str())
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.path, "test.md");
        assert!(!response.content_hash.is_empty());

        // Verify the file was updated
        let updated = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert!(updated.contains("type: updated"));
        assert!(updated.contains("new_field: true"));
        // Original field should still be there
        assert!(updated.contains("tags:"));
    }

    #[tokio::test]
    async fn test_update_with_wrong_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let initial_content = "---\ntype: note\n---\n\nContent here";
        create_test_note(temp_dir.path(), "test.md", initial_content).await;
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let mut updates = HashMap::new();
        updates.insert("type".to_string(), JsonValue::String("updated".to_string()));

        let result = execute(&storage, &graph, "test", updates, "wrong_hash").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note modified since last read"));
    }

    #[tokio::test]
    async fn test_add_frontmatter_to_note_without_frontmatter() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let initial_content = "Just content, no frontmatter";
        create_test_note(temp_dir.path(), "test.md", initial_content).await;
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(initial_content);

        let mut updates = HashMap::new();
        updates.insert("type".to_string(), JsonValue::String("new".to_string()));

        execute(&storage, &graph, "test", updates, content_hash.as_str())
            .await
            .expect("should succeed");

        let updated = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert!(updated.starts_with("---\n"));
        assert!(updated.contains("type: new"));
        assert!(updated.contains("Just content, no frontmatter"));
    }

    #[tokio::test]
    async fn test_update_note_in_subfolder() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let initial_content = "---\ntype: project\n---\n\nProject content";
        create_test_note(temp_dir.path(), "projects/MyProject.md", initial_content).await;
        graph.update_note(
            "MyProject",
            PathBuf::from("projects/MyProject.md"),
            HashSet::new(),
        );

        let content_hash = ContentHash::from_content(initial_content);

        let mut updates = HashMap::new();
        updates.insert(
            "status".to_string(),
            JsonValue::String("active".to_string()),
        );

        let result = execute(
            &storage,
            &graph,
            "projects/MyProject",
            updates,
            content_hash.as_str(),
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.path, "projects/MyProject.md");

        let updated = fs::read_to_string(temp_dir.path().join("projects/MyProject.md"))
            .await
            .unwrap();
        assert!(updated.contains("status: active"));
        assert!(updated.contains("type: project"));
    }

    #[tokio::test]
    async fn test_nonexistent_file_returns_error() {
        let (_temp_dir, storage, graph) = create_test_env().await;

        let mut updates = HashMap::new();
        updates.insert("type".to_string(), JsonValue::String("test".to_string()));

        let result = execute(&storage, &graph, "nonexistent", updates, "some_hash").await;
        assert!(result.is_err());
    }
}
