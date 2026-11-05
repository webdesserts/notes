//! ReadNote tool - read note content with content hash for optimistic locking.

use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use std::fmt::Write;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::sections::outline::{build_outline, extract_section};
use crate::sections::path::{SectionResolveError, resolve_section};
use crate::storage::{ContentHash, Storage, StorageError};

/// Format content with `cat -n` style line numbers (right-aligned number + tab).
pub fn number_lines(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let total = lines.len();
    let width = total.to_string().len().max(1);

    let mut result = String::with_capacity(content.len() + total * (width + 2));
    for (i, line) in lines.iter().enumerate() {
        if i > 0 {
            result.push('\n');
        }
        let _ = write!(result, "{:>width$}\t{}", i + 1, line, width = width);
    }
    result
}

/// Response from ReadNote tool.
#[derive(Serialize)]
pub struct ReadNoteResponse {
    /// The content of the note
    pub content: String,
    /// Content hash for optimistic locking - pass this to write_note
    pub content_hash: String,
    /// The section's full canonical path, present only when a section was
    /// read. Re-submit this (rather than the original short input) on a
    /// follow-up section write to stay robust if a later heading addition
    /// makes the original input newly ambiguous.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_path: Option<String>,
}

/// Execute the ReadNote tool for a whole-note read.
///
/// Thin wrapper over [`execute_scoped`] with `section: None` - kept as a
/// distinct, unchanged entry point so every existing caller and test
/// continues to compile and pass without modification.
// kept: the MCP tool handler in main.rs calls execute_scoped directly, so
// this has no production caller, but every existing test (in this file and
// the cross-file replace_in_note/write_note read-then-write flows)
// still calls execute() unchanged, matching the Storage-trait precedent above.
#[allow(dead_code)]
pub async fn execute<S: Storage>(
    storage: &S,
    graph: &GraphIndex,
    note: &str,
) -> Result<CallToolResult, ErrorData> {
    execute_scoped(storage, graph, note, None).await
}

/// Execute the ReadNote tool, optionally scoped to a single section.
///
/// When `section` is `None`, returns the whole note's content and content
/// hash for subsequent writes; `resolved_path` is omitted from the response
/// JSON entirely.
///
/// When `section` is `Some(path)`, resolves `path` against the note's outline
/// and returns just that section's content (line-numbered starting at 1,
/// relative to the section) with a hash of that section's raw extracted
/// slice (computed before renumbering - the hash always covers exactly what
/// `extract_section` returns, per the Extraction contract) and the section's
/// full canonical `resolved_path`.
pub async fn execute_scoped<S: Storage>(
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    section: Option<&str>,
) -> Result<CallToolResult, ErrorData> {
    // Resolve the note reference
    let (uri, exists) = resolve_note_uri(storage, graph, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    if !exists {
        return Err(ErrorData::invalid_params(
            format!("Note not found: {}", note),
            None,
        ));
    }

    // Read the note
    let (content, _metadata) = storage.read(&uri).await.map_err(|e| match e {
        StorageError::NotFound { .. } => {
            // Race condition - file was deleted between resolve and read
            ErrorData::internal_error("Note was deleted during read", None)
        }
        _ => ErrorData::internal_error(format!("Failed to read note: {}", e), None),
    })?;

    let Some(section_path) = section else {
        // Whole-note read - this is exactly today's logic, reached either via
        // execute()'s wrapper or directly via execute_scoped(.., None).
        let content_hash = ContentHash::from_content(&content);
        let response = ReadNoteResponse {
            content: number_lines(&content),
            content_hash: content_hash.as_str().to_string(),
            resolved_path: None,
        };
        let json = serde_json::to_string(&response).map_err(|e| {
            ErrorData::internal_error(format!("Failed to serialize response: {}", e), None)
        })?;
        return Ok(CallToolResult::success(vec![Content::text(json)]));
    };

    let outline = build_outline(&content);
    let matched_section = resolve_section(&outline, section_path).map_err(|e| match e {
        SectionResolveError::NotFound { path } => ErrorData::invalid_params(
            format!(
                "Section not found: {}. Use the outline tool to see available sections.",
                path
            ),
            None,
        ),
        SectionResolveError::Ambiguous { path, candidates } => ErrorData::invalid_params(
            format!(
                "Section path '{}' is ambiguous, matches: {}. Use a longer path to disambiguate.",
                path,
                candidates.join(", ")
            ),
            None,
        ),
    })?;

    // Hash the raw extraction before renumbering - the hash always covers
    // exactly what extract_section returns, never a display copy.
    let section_content = extract_section(&content, matched_section);
    let content_hash = ContentHash::from_content(&section_content);
    let resolved_path = matched_section.path.clone();

    let response = ReadNoteResponse {
        content: number_lines(&section_content),
        content_hash: content_hash.as_str().to_string(),
        resolved_path: Some(resolved_path),
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
        content: String,
        content_hash: String,
        #[serde(default)]
        resolved_path: Option<String>,
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

    #[tokio::test]
    async fn test_read_existing_note() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create a note
        fs::write(temp_dir.path().join("test.md"), "Hello, world!")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tHello, world!");
        // Hash should be present and non-empty
        assert!(!response.content_hash.is_empty());
    }

    #[tokio::test]
    async fn test_read_returns_consistent_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Content";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        // Read twice - should get same hash
        let result1 = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");
        let result2 = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response1 = parse_response(&result1);
        let response2 = parse_response(&result2);

        assert_eq!(response1.content_hash, response2.content_hash);
    }

    #[tokio::test]
    async fn test_read_note_in_subdirectory() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create subdirectory and note
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("knowledge/My Note.md"), "Note content")
            .await
            .unwrap();
        graph.update_note(
            "My Note",
            PathBuf::from("knowledge/My Note.md"),
            HashSet::new(),
        );

        let result = execute(&storage, &graph, "My Note")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tNote content");
    }

    #[tokio::test]
    async fn test_read_nonexistent_note_returns_error() {
        let (_temp_dir, storage, graph) = create_test_env().await;

        let result = execute(&storage, &graph, "nonexistent").await;

        // Should return an error, not success
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note not found"));
    }

    #[tokio::test]
    async fn test_read_with_wiki_link_syntax() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Content")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "[[test]]")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tContent");
    }

    #[tokio::test]
    async fn test_read_with_memory_uri() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("knowledge/test.md"), "Content")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("knowledge/test.md"), HashSet::new());

        let result = execute(&storage, &graph, "memory:knowledge/test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tContent");
    }

    #[tokio::test]
    async fn test_read_multiline_content_has_line_numbers() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "line one\nline two\nline three",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tline one\n2\tline two\n3\tline three");
    }

    #[tokio::test]
    async fn test_read_empty_content() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\t");
    }

    #[tokio::test]
    async fn test_line_numbers_right_aligned() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // 10+ lines to test right-alignment
        let content = (1..=12)
            .map(|i| format!("line {}", i))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(temp_dir.path().join("test.md"), &content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        // Single-digit lines should be right-aligned with space padding
        assert!(response.content.starts_with(" 1\tline 1\n"));
        assert!(response.content.contains("12\tline 12"));
    }

    #[tokio::test]
    async fn test_section_read_returns_sliced_and_relatively_renumbered_content() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# Top\nintro\n## Middle\nmiddle line one\nmiddle line two\n# Top Two\nmore";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Top > Middle"))
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        // Line numbers are relative to the section: line 1 is the section's
        // own heading line, not its absolute position in the file (line 3).
        assert_eq!(
            response.content,
            "1\t## Middle\n2\tmiddle line one\n3\tmiddle line two"
        );

        let expected_hash = crate::storage::ContentHash::from_content(
            "## Middle\nmiddle line one\nmiddle line two",
        );
        assert_eq!(response.content_hash, expected_hash.as_str());
        assert_eq!(response.resolved_path.as_deref(), Some("Top > Middle"));
    }

    #[tokio::test]
    async fn test_whole_note_via_execute_scoped_omits_resolved_path_from_json() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Hello, world!")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", None)
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(
            json.as_object().unwrap().get("resolved_path").is_none(),
            "resolved_path key should be entirely absent from a whole-note response JSON"
        );
    }

    #[tokio::test]
    async fn test_section_not_found_names_path_and_mentions_outline() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# Heading\nbody")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Nonexistent")).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Nonexistent"));
        assert!(err.message.to_lowercase().contains("outline"));
    }

    #[tokio::test]
    async fn test_ambiguous_section_path_lists_candidates() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "# Notes\nfirst\n# Notes\nsecond",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Notes")).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("ambiguous"));
        // Both candidates named "Notes" should appear in the error message
        // (plus the input itself, so at least 3 occurrences).
        assert!(err.message.matches("Notes").count() >= 3);
    }

    #[tokio::test]
    async fn test_frontmatter_pseudo_section_read() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "---\ntitle: Test\n---\nbody",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Frontmatter"))
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\t---\n2\ttitle: Test\n3\t---");
        assert_eq!(response.resolved_path.as_deref(), Some("Frontmatter"));
    }

    #[tokio::test]
    async fn test_preamble_pseudo_section_read() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "---\ntitle: Test\n---\npreamble text\n# Heading\nbody",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Preamble"))
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\tpreamble text");
        assert_eq!(response.resolved_path.as_deref(), Some("Preamble"));
    }

    #[tokio::test]
    async fn test_unambiguous_full_path_succeeds_despite_duplicate_named_headings_elsewhere() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Two top-level sections each with a child named "Details" - a bare
        // "Details" would be ambiguous, but the full path disambiguates.
        let content = "\
# Alpha
## Details
alpha details
# Beta
## Details
beta details";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Beta > Details"))
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.content, "1\t## Details\n2\tbeta details");
        assert_eq!(response.resolved_path.as_deref(), Some("Beta > Details"));
    }

    #[tokio::test]
    async fn test_resolved_path_is_full_canonical_path_not_short_input() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // "Configure" is a bare, unique heading name here - shorter than the
        // section's full ancestor-chain path.
        let content = "# Top\n## Configure\nbody";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(&storage, &graph, "test", Some("Configure"))
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        // resolved_path is the section's full canonical path, not the short
        // input the agent actually typed.
        assert_eq!(response.resolved_path.as_deref(), Some("Top > Configure"));
    }
}
