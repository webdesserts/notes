//! Outline tool - discover a note's addressable sections for section-scoped
//! reads and writes on oversized notes.

use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::sections::outline::{SectionKind, build_outline};
use crate::storage::{Storage, StorageError};

/// A single section entry in the outline response.
///
/// Deliberately carries no hash field: an outline-supplied hash would enable
/// blind writes (a hash for content the agent hasn't actually read). Reads
/// and writes each derive their own section hash from content they fetch
/// themselves.
#[derive(Serialize)]
pub struct OutlineSectionEntry {
    /// The literal string to pass as the `section` param of `read_note` or
    /// `write_note`.
    pub path: String,
    /// "heading" | "frontmatter" | "preamble"
    pub kind: &'static str,
    /// Heading level (1-6 for headings; 0 for the Frontmatter/Preamble pseudo-sections).
    pub level: u8,
    /// Heading text ("Frontmatter"/"Preamble" for pseudo-sections).
    pub text: String,
    /// First line of the section, 1-indexed, inclusive.
    pub start_line: usize,
    /// Last line of the section, 1-indexed, inclusive.
    pub end_line: usize,
    /// Character count of the section's content.
    pub size_chars: usize,
}

/// Response from the Outline tool.
#[derive(Serialize)]
pub struct OutlineResponse {
    /// The memory URI of the note.
    pub uri: String,
    /// Flat list of addressable sections in document order. `level` plus the
    /// full `path` chain already convey nesting.
    pub sections: Vec<OutlineSectionEntry>,
}

fn kind_str(kind: SectionKind) -> &'static str {
    match kind {
        SectionKind::Frontmatter => "frontmatter",
        SectionKind::Preamble => "preamble",
        SectionKind::Heading => "heading",
    }
}

/// Execute the Outline tool.
///
/// Returns the flat list of a note's addressable sections (frontmatter,
/// preamble, and heading-delimited sections). `path` values in the response
/// are the literal strings to pass as the `section` param of `read_note` or
/// `write_note`.
pub async fn execute<S: Storage>(
    storage: &S,
    graph: &GraphIndex,
    note: &str,
) -> Result<CallToolResult, ErrorData> {
    let (uri, exists) = resolve_note_uri(storage, graph, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    if !exists {
        return Err(ErrorData::invalid_params(
            format!("Note not found: {}", note),
            None,
        ));
    }

    let (content, _metadata) = storage.read(&uri).await.map_err(|e| match e {
        StorageError::NotFound { .. } => {
            // Race condition - file was deleted between resolve and read
            ErrorData::internal_error("Note was deleted during read", None)
        }
        _ => ErrorData::internal_error(format!("Failed to read note: {}", e), None),
    })?;

    let outline = build_outline(&content);

    let sections = outline
        .sections
        .into_iter()
        .map(|section| OutlineSectionEntry {
            path: section.path,
            kind: kind_str(section.kind),
            level: section.level,
            text: section.text,
            start_line: section.start_line,
            end_line: section.end_line,
            size_chars: section.size_chars,
        })
        .collect();

    let response = OutlineResponse {
        uri: format!("memory:{}", uri),
        sections,
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
    use std::collections::HashSet;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    async fn create_test_env() -> (TempDir, FileStorage, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path().to_path_buf());
        let graph = GraphIndex::new();
        (temp_dir, storage, graph)
    }

    fn parse_response(result: &CallToolResult) -> serde_json::Value {
        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();
        serde_json::from_str(&text).expect("Expected valid JSON")
    }

    #[tokio::test]
    async fn test_multi_section_note_round_trip() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "---\ntitle: Test\n---\n# Top\nintro\n## Middle\nbody";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response["uri"], "memory:test");
        let sections = response["sections"].as_array().unwrap();
        assert_eq!(sections.len(), 3);

        assert_eq!(sections[0]["path"], "Frontmatter");
        assert_eq!(sections[0]["kind"], "frontmatter");
        assert_eq!(sections[0]["level"], 0);

        assert_eq!(sections[1]["path"], "Top");
        assert_eq!(sections[1]["kind"], "heading");
        assert_eq!(sections[1]["level"], 1);
        assert_eq!(sections[1]["text"], "Top");
        assert_eq!(sections[1]["start_line"], 4);

        assert_eq!(sections[2]["path"], "Top > Middle");
        assert_eq!(sections[2]["kind"], "heading");
        assert_eq!(sections[2]["level"], 2);
    }

    #[tokio::test]
    async fn test_nonexistent_note_returns_not_found_error() {
        let (_temp_dir, storage, graph) = create_test_env().await;

        let result = execute(&storage, &graph, "nonexistent").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note not found"));
    }

    #[tokio::test]
    async fn test_zero_heading_note_is_preamble_only() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "Just some prose.")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let response = parse_response(&result);
        let sections = response["sections"].as_array().unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0]["path"], "Preamble");
        assert_eq!(sections[0]["kind"], "preamble");
    }

    #[tokio::test]
    async fn test_response_json_has_no_hash_looking_content() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "---\ntitle: Test\n---\n# Heading\nbody text";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute(&storage, &graph, "test")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Regression pin against a future field addition: no key name should
        // mention "hash", and no string value should look like a SHA-256 hex
        // digest (64 hex chars).
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_no_hash_shaped_content(&json);
    }

    /// Recursively assert no JSON key contains "hash" and no string value is
    /// shaped like a SHA-256 hex digest.
    fn assert_no_hash_shaped_content(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    assert!(
                        !key.to_lowercase().contains("hash"),
                        "found a hash-named key in outline response: {}",
                        key
                    );
                    assert_no_hash_shaped_content(val);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    assert_no_hash_shaped_content(item);
                }
            }
            serde_json::Value::String(s) => {
                let is_hash_shaped = s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit());
                assert!(
                    !is_hash_shaped,
                    "found a hash-shaped string in outline response: {}",
                    s
                );
            }
            _ => {}
        }
    }
}
