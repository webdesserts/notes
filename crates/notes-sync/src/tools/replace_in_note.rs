//! ReplaceInNote tool - make surgical text replacements in a note.
//!
//! Hash-free by contract: the caller supplies no content hash. The note is
//! read fresh for this operation, the whole batch is validated against that
//! read in memory, and the write itself is guarded by an internal CAS on the
//! freshly-read whole-file hash (the caller never sees or supplies it).
//!
//! Replacement semantics come from the shared pure kernel in `notes-core`
//! (`notes_core::replace`), which is also exercised byte-for-byte by the
//! canonical fixture at `crates/notes-core/test-fixtures/replace-behavior.json`.

use notes_core::replace::{ReplacementEdit, apply_replacements};
use notes_core::sections::write::{SectionWriteError, resolve_section_for_edit, splice_section};

use notes_core::note_ref::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use std::path::Path;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::storage::{ContentHash, Storage, StorageError};

/// A single edit operation.
#[derive(Debug, Clone)]
pub struct Edit {
    /// Text to search for - must identify exactly one possible match
    /// (overlapping occurrences all count) in the selected scope.
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
    /// Hash of the replaced scope - the whole modified note for an unscoped
    /// replace, the modified section's content when `section` was given.
    /// This mirrors write_note's section-edit response, which also reports
    /// the section hash, not the whole-file hash. The whole-file fresh hash
    /// used for the internal write guard stays internal.
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
    /// Hash the replaced scope would have after applying the edits - the
    /// whole modified note for an unscoped replace, the modified section's
    /// content when `section` was given. No write happens in a dry run.
    pub would_produce_hash: String,
    /// Number of edits that would be applied
    pub edits_count: usize,
    /// Description of changes
    pub changes: String,
}

/// Build the human-readable description of a fully-validated batch.
fn changes_description(edits: &[Edit]) -> String {
    if edits.is_empty() {
        return "No changes made.".to_string();
    }
    let changes: Vec<String> = edits
        .iter()
        .map(|edit| {
            format!(
                "- Replaced:\n  {}\n  With:\n  {}",
                truncate_for_display(&edit.old_text, 60),
                truncate_for_display(&edit.new_text, 60)
            )
        })
        .collect();
    changes.join("\n\n")
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
/// note's whole content, or - when `section` is given - against the resolved
/// section only. Each oldText must identify exactly one possible match in the
/// selected scope; identical text elsewhere is irrelevant for a scoped
/// replace. No caller hash is accepted: the note is read fresh here, and a
/// concurrent out-of-band change is refused at write time by the internal
/// whole-file hash CAS (concurrency with external writers is memory/t:10's
/// concern, not this tool's contract).
///
/// Edits apply progressively in memory (later edits see earlier output), but
/// the entire batch validates before anything is written - a failure on any
/// edit, including one after earlier edits already succeeded in memory,
/// leaves the note byte-for-byte unchanged.
///
/// The returned hash describes the replaced scope consistently with
/// write_note's outcomes: whole-note hash when unscoped, the modified
/// section's hash when scoped.
pub async fn execute<S: Storage>(
    _vault_path: &Path,
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    edits: Vec<Edit>,
    section: Option<&str>,
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

    // Read current content (note existence already verified by resolve_note_uri).
    // This fresh full read is the single basis for everything below: section
    // resolution, edit validation, and the write-time CAS hash.
    let (content, _metadata) = storage
        .read(&uri)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to read note: {}", e), None))?;

    // Resolve the section once from this operation's fresh read, if scoped.
    // Match only inside the resolved region; unknown/ambiguous paths refuse
    // before any edit runs, and siblings are never re-resolved or touched.
    let (scope_content, splice_range) = match section {
        None => (content.clone(), None),
        Some(path) => {
            let resolved = resolve_section_for_edit(&content, path).map_err(|e| match e {
                SectionWriteError::NotFound { path } => ErrorData::invalid_params(
                    format!(
                        "Section not found: {}. Use the outline tool to see available sections.",
                        path
                    ),
                    None,
                ),
                SectionWriteError::Ambiguous { path, candidates } => ErrorData::invalid_params(
                    format!(
                        "Section path '{}' is ambiguous, matches: {}. Use a longer path \
                         to disambiguate.",
                        path,
                        candidates.join(", ")
                    ),
                    None,
                ),
                // Unreachable: resolve_section_for_edit performs no hash
                // verification. Kept total so adapter error mapping stays
                // exhaustive over the shared error type.
                SectionWriteError::HashMismatch { .. } => ErrorData::internal_error(
                    "section resolution unexpectedly reported a hash mismatch".to_string(),
                    None,
                ),
            })?;
            (
                resolved.section_content,
                Some((resolved.start_line, resolved.end_line)),
            )
        }
    };

    // Validate and apply the whole batch in memory against the selected
    // scope. Any zero-match or ambiguous edit refuses the entire operation
    // before any write.
    let kernel_edits: Vec<ReplacementEdit> = edits
        .iter()
        .map(|e| ReplacementEdit {
            old_text: e.old_text.clone(),
            new_text: e.new_text.clone(),
        })
        .collect();
    let modified_scope = apply_replacements(&scope_content, &kernel_edits).map_err(|rejected| {
        match rejected.error {
            notes_core::ReplacementError::NotFound { old_text } => ErrorData::invalid_params(
                format!(
                    "Edit failed: Could not find text to replace:\n{}",
                    truncate_for_display(&old_text, 100)
                ),
                None,
            ),
            notes_core::ReplacementError::Ambiguous { old_text, count } => {
                ErrorData::invalid_params(
                    format!(
                        "Edit failed: Text appears {} times in note - edit would be ambiguous:\n{}",
                        count,
                        truncate_for_display(&old_text, 100)
                    ),
                    None,
                )
            }
        }
    })?;

    // Scoped: splice the modified section back into the full content exactly
    // once. Unscoped: the modified scope IS the full content.
    let new_full_content = match splice_range {
        Some((start_line, end_line)) => {
            splice_section(&content, start_line, end_line, &modified_scope)
        }
        None => modified_scope.clone(),
    };

    // The scope hash is what the response reports (whole-note hash when
    // unscoped - identical to the full-file hash in that case).
    let new_scope_hash = ContentHash::from_content(&modified_scope);

    if dry_run {
        let response = ReplaceInNoteDryRunResponse {
            uri: format!("memory:{}", uri),
            path: ensure_markdown_extension(&uri),
            would_produce_hash: new_scope_hash.as_str().to_string(),
            edits_count: edits.len(),
            changes: changes_description(&edits),
        };
        let json = serde_json::to_string(&response).map_err(|e| {
            ErrorData::internal_error(format!("Failed to serialize response: {}", e), None)
        })?;
        return Ok(CallToolResult::success(vec![Content::text(json)]));
    }

    // Write the modified content with optimistic locking (TOCTOU protection):
    // the expected hash is the fresh full-file hash read for THIS operation -
    // an internal guard, not a caller token. A mismatch can only mean the
    // file changed out-of-band between the read above and this write.
    let fresh_full_hash = ContentHash::from_content(&content);
    storage
        .write(&uri, &new_full_content, Some(fresh_full_hash.as_str()))
        .await
        .map_err(|e| match e {
            StorageError::HashMismatch { .. } => {
                ErrorData::invalid_params(MODIFIED_SINCE_READ.to_string(), None)
            }
            _ => ErrorData::internal_error(format!("Failed to write note: {}", e), None),
        })?;

    let response = ReplaceInNoteResponse {
        uri: format!("memory:{}", uri),
        path: ensure_markdown_extension(&uri),
        content_hash: new_scope_hash.as_str().to_string(),
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

    // The canonical behavioral fixture shared with the native adapter: every
    // case runs through this storage-level execute, asserting exact resulting
    // bytes on success and exact refusal (disk untouched) on failure.
    const FIXTURE_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../notes-core/test-fixtures/replace-behavior.json"
    );

    #[derive(Deserialize)]
    struct Fixture {
        cases: Vec<FixtureCase>,
    }

    #[derive(Deserialize)]
    struct FixtureCase {
        id: String,
        content: String,
        #[serde(default)]
        section: Option<String>,
        edits: Vec<FixtureEdit>,
        expect: String,
        #[serde(default)]
        error: Option<String>,
        #[serde(rename = "expectedContent")]
        expected_content: String,
    }

    #[derive(Deserialize)]
    struct FixtureEdit {
        #[serde(rename = "oldText")]
        old_text: String,
        #[serde(rename = "newText")]
        new_text: String,
    }

    fn load_fixture() -> Fixture {
        let raw = std::fs::read_to_string(FIXTURE_PATH)
            .unwrap_or_else(|e| panic!("canonical fixture must be readable: {e}"));
        serde_json::from_str(&raw).expect("canonical fixture must parse")
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
    async fn test_canonical_fixture_cases_through_execute() {
        let fixture = load_fixture();

        for case in &fixture.cases {
            let (temp_dir, storage, mut graph) = create_test_env().await;

            fs::write(temp_dir.path().join("fixture.md"), &case.content)
                .await
                .unwrap();
            graph.update_note("fixture", PathBuf::from("fixture.md"), HashSet::new());

            let edits: Vec<Edit> = case
                .edits
                .iter()
                .map(|e| Edit {
                    old_text: e.old_text.clone(),
                    new_text: e.new_text.clone(),
                })
                .collect();

            let result = execute(
                temp_dir.path(),
                &storage,
                &graph,
                "fixture",
                edits,
                case.section.as_deref(),
                false,
            )
            .await;

            match case.expect.as_str() {
                "ok" => {
                    result.unwrap_or_else(|e| {
                        panic!("case {} should succeed: {}", case.id, e.message)
                    });
                }
                "error" => {
                    let err = result.expect_err("case must be refused");
                    let kind = case
                        .error
                        .as_deref()
                        .unwrap_or_else(|| panic!("error case {} needs a kind", case.id));
                    let needle = match kind {
                        "not-found" => "Could not find text to replace",
                        "ambiguous" => "edit would be ambiguous",
                        "section-not-found" => "Section not found",
                        "section-ambiguous" => "is ambiguous, matches",
                        other => panic!("case {}: unknown error kind {other}", case.id),
                    };
                    assert!(
                        err.message.contains(needle),
                        "case {}: expected {kind} error mentioning '{needle}', got: {}",
                        case.id,
                        err.message
                    );
                }
                other => panic!("case {}: unknown expect kind {other}", case.id),
            }

            // Every case asserts the exact on-disk bytes: the spliced result
            // for ok cases, and byte-for-byte unchanged content for refusals
            // (including a batch that failed after an earlier in-memory edit).
            let on_disk = fs::read_to_string(temp_dir.path().join("fixture.md"))
                .await
                .unwrap();
            assert_eq!(
                on_disk, case.expected_content,
                "case {}: on-disk bytes must match the fixture exactly",
                case.id
            );
        }
    }

    #[tokio::test]
    async fn test_edit_single_replacement() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

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
            None,
            false,
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.uri, "memory:test");
        assert_eq!(response.edits_applied, 1);
        // Unscoped response hash describes the whole modified note.
        assert_eq!(
            response.content_hash,
            ContentHash::from_content("Hello, Rust!").as_str()
        );

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
            None,
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
            None,
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
            None,
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("appears 2 times"));
    }

    #[tokio::test]
    async fn test_edit_fails_if_text_overlapping_ambiguous() {
        // `aa` fits at two overlapping positions in `aaa` - ambiguous even
        // though a non-overlapping match count would report one.
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "aaa")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "aa".to_string(),
            new_text: "b".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            None,
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("appears 2 times"));

        let on_disk = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, "aaa");
    }

    #[tokio::test]
    async fn test_edit_section_scoped() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# Intro\n\nshared phrase outside\n\n## Details\n\nshared phrase inside\n";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "shared phrase".to_string(),
            new_text: "REPLACED".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            Some("Details"),
            false,
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.edits_applied, 1);
        // Scoped response hash describes the modified section, not the file.
        let new_section = "## Details\n\nREPLACED inside\n";
        assert_eq!(
            response.content_hash,
            ContentHash::from_content(new_section).as_str()
        );
        assert_ne!(
            response.content_hash,
            ContentHash::from_content(
                "# Intro\n\nshared phrase outside\n\n## Details\n\nREPLACED inside\n"
            )
            .as_str()
        );

        // Only the section changed; identical text outside is untouched.
        let on_disk = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(
            on_disk,
            "# Intro\n\nshared phrase outside\n\n## Details\n\nREPLACED inside\n"
        );
    }

    #[tokio::test]
    async fn test_edit_section_not_found() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# A\n\nbody\n";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "body".to_string(),
            new_text: "replaced".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            Some("Missing"),
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Section not found"));

        let on_disk = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, content);
    }

    #[tokio::test]
    async fn test_edit_section_ambiguous() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# Notes\n\nfirst\n\n# Notes\n\nsecond\n";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "first".to_string(),
            new_text: "replaced".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            Some("Notes"),
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("is ambiguous, matches"));

        let on_disk = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, content);
    }

    #[tokio::test]
    async fn test_edit_dry_run() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "Hello, world!";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "world".to_string(),
            new_text: "Rust".to_string(),
        }];

        let result = execute(temp_dir.path(), &storage, &graph, "test", edits, None, true)
            .await
            .expect("should succeed");

        let response = parse_dry_run_response(&result);
        assert_eq!(response.uri, "memory:test");
        // Dry-run hash meaning matches the write response: the replaced
        // scope's hash (whole note here).
        assert_eq!(
            response.would_produce_hash,
            ContentHash::from_content("Hello, Rust!").as_str()
        );
        assert_eq!(response.edits_count, 1);
        assert!(response.changes.contains("Replaced"));

        // Verify content was NOT changed
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hello, world!");
    }

    #[tokio::test]
    async fn test_edit_dry_run_scoped_hash_means_section() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# Intro\n\npreamble\n\n## Details\n\nsection body\n";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let edits = vec![Edit {
            old_text: "section body".to_string(),
            new_text: "edited body".to_string(),
        }];

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            edits,
            Some("Details"),
            true,
        )
        .await
        .expect("should succeed");

        let response = parse_dry_run_response(&result);
        // Scoped dry-run hash describes the modified section content.
        assert_eq!(
            response.would_produce_hash,
            ContentHash::from_content("## Details\n\nedited body\n").as_str()
        );

        // Dry run never writes.
        let on_disk = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, content);
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
            None,
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

        // First edit - no hash input required, even for a first call that
        // never read the note.
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
            None,
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
            None,
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

        // Step 1: ReadNote - the returned hash is for chained overwrite
        // tools; replace_in_note itself no longer accepts any hash.
        let read_result = super::super::read_note::execute(&storage, &graph, "My Note")
            .await
            .expect("ReadNote should succeed");

        let read_json: serde_json::Value =
            serde_json::from_str(&read_result.content[0].raw.as_text().unwrap().text).unwrap();

        assert_eq!(read_json["content"].as_str().unwrap(), "1\tHello, world!");

        // Step 2: ReplaceInNote with no hash input at all
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
            None,
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
