//! ReplaceInNote tool - make surgical text replacements in a note.
//!
//! Based on the MCP filesystem server's edit_file implementation,
//! this tool uses oldText/newText pairs for precise edits.

use obsidian_fs::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use std::path::Path;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::storage::{ContentHash, Storage, StorageError};

/// A single edit operation.
#[derive(Debug, Clone)]
pub struct Edit {
    /// Text to search for - must match exactly
    pub old_text: String,
    /// Text to replace with
    pub new_text: String,
}

/// Response from ReplaceInNote tool.
#[derive(Serialize)]
pub struct ReplaceInNoteResponse {
    /// The memory URI of the note
    pub uri: String,
    /// The file path relative to vault
    pub path: String,
    /// New content hash after edit - use this for subsequent edits
    pub content_hash: String,
    /// Number of edits applied
    pub edits_applied: usize,
}

/// Response from ReplaceInNote dry run.
#[derive(Serialize)]
pub struct ReplaceInNoteDryRunResponse {
    /// The memory URI of the note
    pub uri: String,
    /// The file path relative to vault
    pub path: String,
    /// Hash that would result from applying edits
    pub would_produce_hash: String,
    /// Number of edits that would be applied
    pub edits_count: usize,
    /// Description of changes
    pub changes: String,
}

/// Apply edits to content, returning the modified content and a diff.
fn apply_edits(content: &str, edits: &[Edit]) -> Result<(String, String), String> {
    let mut modified = content.to_string();
    let mut changes = Vec::new();

    for edit in edits {
        if !modified.contains(&edit.old_text) {
            return Err(format!(
                "Could not find text to replace:\n{}",
                truncate_for_display(&edit.old_text, 100)
            ));
        }

        // Count occurrences
        let count = modified.matches(&edit.old_text).count();
        if count > 1 {
            return Err(format!(
                "Text appears {} times in note - edit would be ambiguous:\n{}",
                count,
                truncate_for_display(&edit.old_text, 100)
            ));
        }

        modified = modified.replacen(&edit.old_text, &edit.new_text, 1);
        changes.push(format!(
            "- Replaced:\n  {}\n  With:\n  {}",
            truncate_for_display(&edit.old_text, 60),
            truncate_for_display(&edit.new_text, 60)
        ));
    }

    let diff = if changes.is_empty() {
        "No changes made.".to_string()
    } else {
        changes.join("\n\n")
    };

    Ok((modified, diff))
}

/// Truncate a string for display, adding ellipsis if needed.
fn truncate_for_display(s: &str, max_len: usize) -> String {
    let trimmed = s.trim();
    let char_count = trimmed.chars().count();
    if char_count <= max_len {
        trimmed.replace('\n', "\\n")
    } else {
        let truncated: String = trimmed.chars().take(max_len).collect();
        format!("{}...", truncated.replace('\n', "\\n"))
    }
}

/// The generic "modified since last read" message shared by every hash
/// mismatch path.
const MODIFIED_SINCE_READ: &str = "Note modified since last read. Read the note again to get the \
     current content and hash before retrying.";

/// Execute the ReplaceInNote tool.
///
/// Makes surgical text replacements using oldText/newText pairs against a
/// note's whole content. Each oldText must appear exactly once.
pub async fn execute<S: Storage>(
    _vault_path: &Path,
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    edits: Vec<Edit>,
    content_hash: &str,
    dry_run: bool,
) -> Result<CallToolResult, ErrorData> {
    // Resolve the note reference using the same logic as read_note
    let (uri, exists) = resolve_note_uri(storage, graph, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    if !exists {
        return Err(ErrorData::invalid_params(
            format!("Note not found: {}", note),
            None,
        ));
    }

    // Read current content (note existence already verified by resolve_note_uri)
    let (content, _metadata) = storage
        .read(&uri)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to read note: {}", e), None))?;

    let current_hash = ContentHash::from_content(&content);
    if current_hash.as_str() != content_hash {
        return Err(ErrorData::invalid_params(
            MODIFIED_SINCE_READ.to_string(),
            None,
        ));
    }

    let (modified, diff) = apply_edits(&content, &edits)
        .map_err(|e| ErrorData::invalid_params(format!("Edit failed: {}", e), None))?;

    let file_path = ensure_markdown_extension(&uri);
    let new_hash = ContentHash::from_content(&modified);

    if dry_run {
        let response = ReplaceInNoteDryRunResponse {
            uri: format!("memory:{}", uri),
            path: file_path,
            would_produce_hash: new_hash.as_str().to_string(),
            edits_count: edits.len(),
            changes: diff,
        };
        let json = serde_json::to_string(&response).map_err(|e| {
            ErrorData::internal_error(format!("Failed to serialize response: {}", e), None)
        })?;
        return Ok(CallToolResult::success(vec![Content::text(json)]));
    }

    // Write the modified content with optimistic locking (TOCTOU protection)
    storage
        .write(&uri, &modified, Some(content_hash))
        .await
        .map_err(|e| match e {
            StorageError::HashMismatch { .. } => {
                ErrorData::invalid_params(MODIFIED_SINCE_READ.to_string(), None)
            }
            _ => ErrorData::internal_error(format!("Failed to write note: {}", e), None),
        })?;

    let response = ReplaceInNoteResponse {
        uri: format!("memory:{}", uri),
        path: file_path,
        content_hash: new_hash.as_str().to_string(),
        edits_applied: edits.len(),
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
        uri: String,
        // kept: mirrors the `path` field in the tool's JSON response; documents the
        // response shape even though these assertions don't check it.
        #[allow(dead_code)]
        path: String,
        content_hash: String,
        edits_applied: usize,
    }

    #[derive(Deserialize)]
    struct TestDryRunResponse {
        uri: String,
        would_produce_hash: String,
        edits_count: usize,
        changes: String,
    }

    async fn create_test_env() -> (TempDir, FileStorage, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path().to_path_buf());
        let graph = GraphIndex::new();
        (temp_dir, storage, graph)
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

    fn parse_dry_run_response(result: &CallToolResult) -> TestDryRunResponse {
        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();
        serde_json::from_str(&text).expect("Expected valid JSON")
    }

    #[tokio::test]
    async fn test_edit_with_wrong_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Hello, world!")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        // Should fail with wrong hash
        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            "wrong_hash",
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note modified since last read"));
    }

    #[tokio::test]
    async fn test_edit_single_replacement() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        let edits = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            content_hash.as_str(),
            false,
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.uri, "memory:test");
        assert_eq!(response.edits_applied, 1);
        assert!(!response.content_hash.is_empty());

        // Verify content changed
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hello, Rust!");
    }

    #[tokio::test]
    async fn test_edit_multiple_replacements() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world! Goodbye, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        let edits = vec![
            Edit {
                old_text: "Hello".to_string(),
                new_text: "Hi".to_string(),
            },
            Edit {
                old_text: "Goodbye".to_string(),
                new_text: "Bye".to_string(),
            },
        ];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            content_hash.as_str(),
            false,
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.edits_applied, 2);

        // Verify content changed
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hi, world! Bye, world!");
    }

    #[tokio::test]
    async fn test_edit_fails_if_text_not_found() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        let edits = vec![Edit {
            old_text: "nonexistent".to_string(),
            new_text: "replacement".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            content_hash.as_str(),
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Could not find text"));
    }

    #[tokio::test]
    async fn test_edit_fails_if_text_ambiguous() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "foo bar foo";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        let edits = vec![Edit {
            old_text: "foo".to_string(),
            new_text: "baz".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            content_hash.as_str(),
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("appears 2 times"));
    }

    #[tokio::test]
    async fn test_edit_dry_run() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        let edits = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            content_hash.as_str(),
            true,
        )
        .await
        .expect("should succeed");

        let response = parse_dry_run_response(&result);
        assert_eq!(response.uri, "memory:test");
        assert!(!response.would_produce_hash.is_empty());
        assert_eq!(response.edits_count, 1);
        assert!(response.changes.contains("Replaced"));

        // Verify content was NOT changed
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hello, world!");
    }

    #[tokio::test]
    async fn test_edit_nonexistent_note() {
        let (_temp_dir, storage, graph) = create_test_env().await;

        let edits = vec![Edit {
            old_text: "foo".to_string(),
            new_text: "bar".to_string(),
        }];

        let result = execute(
            _temp_dir.path(),
            &storage,
            &graph,
            "nonexistent",
            edits,
            "some_hash",
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note not found"));
    }

    #[tokio::test]
    async fn test_edit_returns_hash_for_chained_edits() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let content_hash = ContentHash::from_content(content);

        // First edit
        let edits1 = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        let result1 = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits1,
            content_hash.as_str(),
            false,
        )
        .await
        .expect("should succeed");

        let response1 = parse_response(&result1);

        // Second edit using hash from first edit
        let edits2 = vec![Edit {
            old_text: "Hello".to_string(),
            new_text: "Goodbye".to_string(),
        }];

        let result2 = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits2,
            &response1.content_hash,
            false,
        )
        .await
        .expect("should succeed");

        let response2 = parse_response(&result2);
        assert_ne!(response1.content_hash, response2.content_hash);

        // Verify final content
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Goodbye, Rust!");
    }

    // Integration tests - test the actual ReadNote→ReplaceInNote flow

    #[tokio::test]
    async fn test_read_then_edit_flow() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create note in subdirectory
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(
            temp_dir.path().join("knowledge/My Note.md"),
            "Hello, world!",
        )
        .await
        .unwrap();
        graph.update_note(
            "My Note",
            PathBuf::from("knowledge/My Note.md"),
            HashSet::new(),
        );

        // Step 1: ReadNote
        let read_result = super::super::read_note::execute(&storage, &graph, "My Note")
            .await
            .expect("ReadNote should succeed");

        let read_json: serde_json::Value =
            serde_json::from_str(&read_result.content[0].raw.as_text().unwrap().text).unwrap();

        let content_hash = read_json["content_hash"].as_str().unwrap();
        assert_eq!(read_json["content"].as_str().unwrap(), "1\tHello, world!");

        // Step 2: ReplaceInNote with hash from read
        let edits = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        let edit_result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "My Note",
            edits,
            content_hash,
            false,
        )
        .await
        .expect("ReplaceInNote should succeed");

        let response = parse_response(&edit_result);
        assert_eq!(response.uri, "memory:knowledge/My Note");

        // Verify the file was actually modified
        let content = fs::read_to_string(temp_dir.path().join("knowledge/My Note.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hello, Rust!");
    }
}
