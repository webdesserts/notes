//! WriteNote tool - write note content with optimistic locking via content_hash.
//!
//! One tool, mode selected by the hash: whole-note or whole-section, create
//! or overwrite. See [`execute_scoped`] for the full truth table.

use obsidian_fs::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use std::path::Path;

use super::common::resolve_note_uri;
use crate::graph::GraphIndex;
use crate::sections::create::{SectionCreateError, create_section};
use crate::sections::write::{SectionWriteError, resolve_section_for_write, splice_section};
use crate::storage::{ContentHash, Storage, StorageError};

/// Response from WriteNote tool.
#[derive(Serialize)]
pub struct WriteNoteResponse {
    /// The memory URI of the note
    pub uri: String,
    /// The file path relative to vault
    pub path: String,
    /// New content hash after write - use this for subsequent writes
    pub content_hash: String,
    /// Number of bytes written
    pub bytes_written: usize,
}

/// The generic "modified since last read" message shared by every hash
/// mismatch path (whole-note and, via the internal TOCTOU guard, section
/// writes too) - never echoes the current hash.
const MODIFIED_SINCE_READ: &str = "Note modified since last read. Read the note again to get the \
     current content and hash before retrying.";

/// Build the tool's success response and serialize it.
fn success_response(
    uri: &str,
    file_path: String,
    content_hash: &str,
    bytes_written: usize,
) -> Result<CallToolResult, ErrorData> {
    let response = WriteNoteResponse {
        uri: format!("memory:{}", uri),
        path: file_path,
        content_hash: content_hash.to_string(),
        bytes_written,
    };
    let json = serde_json::to_string(&response).map_err(|e| {
        ErrorData::internal_error(format!("Failed to serialize response: {}", e), None)
    })?;
    Ok(CallToolResult::success(vec![Content::text(json)]))
}

/// Map a storage write failure to the tool's error shape. Shared by every
/// branch that calls `storage.write` - `ParentNotFound` can only actually
/// occur for the whole-note/section-create branches (an edit's note already
/// exists, so its parent necessarily does too), but mapping it uniformly
/// keeps this one function the single place write errors are translated.
fn map_write_error(e: StorageError) -> ErrorData {
    match e {
        StorageError::ParentNotFound { uri, parent } => ErrorData::invalid_params(
            format!(
                "Parent directory doesn't exist for '{}': {}. \
                 Create the directory first or use a different path.",
                uri,
                parent.display()
            ),
            None,
        ),
        StorageError::HashMismatch { .. } => {
            ErrorData::invalid_params(MODIFIED_SINCE_READ.to_string(), None)
        }
        _ => ErrorData::internal_error(format!("Failed to write note: {}", e), None),
    }
}

/// Execute the WriteNote tool for a whole-note write.
///
/// Thin wrapper over [`execute_scoped`] with `section: None` - kept as a
/// distinct, unchanged entry point so every existing caller and test
/// continues to compile and pass without modification.
// kept: the MCP tool handler in main.rs calls execute_scoped directly, so
// this has no production caller, but every existing test (in this file and
// cross-file callers) still calls execute() unchanged, matching the
// read_note::execute precedent.
#[allow(dead_code)]
pub async fn execute<S: Storage>(
    vault_path: &Path,
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    content: &str,
    content_hash: Option<&str>,
) -> Result<CallToolResult, ErrorData> {
    execute_scoped(
        vault_path,
        storage,
        graph,
        note,
        content,
        content_hash,
        None,
    )
    .await
}

/// Execute the WriteNote tool, optionally scoped to a single section.
///
/// Full truth table, keyed on whether `content_hash` and `section` are set:
///
/// - `content_hash: None`, `section: None` - create a new note. Fails if the
///   note already exists.
/// - `content_hash: Some`, `section: None` - overwrite an existing note.
///   Fails if the note doesn't exist or the hash is stale.
/// - `content_hash: None`, `section: Some` - **create** a section. `content`
///   is body-only (no heading line); [`create_section`] synthesizes the
///   leaf heading and any missing ancestor headings. Works whether the note
///   already exists (section appended/nested into it) or not (note created
///   fresh). Fails if the section already exists - read it first to get its
///   hash, then edit it instead.
/// - `content_hash: Some`, `section: Some` - **edit** an existing section.
///   `content` is the section's full replacement text, including its own
///   heading line (unlike create, since there's no heading to synthesize -
///   it's already there). The section is re-resolved fresh by path against
///   the current file and hash-verified before splicing, reusing
///   `resolve_section_for_write`/`splice_section`, the same machinery that
///   used to be shared with `edit_note`/`replace_in_note`'s own
///   section-scoped edits before both retired that capability in favor of
///   this tool.
#[allow(clippy::too_many_arguments)]
pub async fn execute_scoped<S: Storage>(
    _vault_path: &Path,
    storage: &S,
    graph: &GraphIndex,
    note: &str,
    content: &str,
    content_hash: Option<&str>,
    section: Option<&str>,
) -> Result<CallToolResult, ErrorData> {
    let (uri, exists) = resolve_note_uri(storage, graph, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    let Some(section_path) = section else {
        // Whole-note write - this is exactly today's logic, reached either
        // via execute()'s wrapper or directly via execute_scoped(.., None).
        if exists {
            match content_hash {
                Some(hash) => {
                    let (current_content, _) = storage.read(&uri).await.map_err(|e| {
                        ErrorData::internal_error(
                            format!("Failed to read note for hash check: {}", e),
                            None,
                        )
                    })?;
                    let current_hash = ContentHash::from_content(&current_content);
                    if current_hash.as_str() != hash {
                        return Err(ErrorData::invalid_params(
                            MODIFIED_SINCE_READ.to_string(),
                            None,
                        ));
                    }
                }
                None => {
                    return Err(ErrorData::invalid_params(
                        "Note already exists. Read it first to get content_hash, then include in write request.".to_string(),
                        None,
                    ));
                }
            }
        } else if content_hash.is_some() {
            return Err(ErrorData::invalid_params(
                format!("Note does not exist: {}", note),
                None,
            ));
        }

        storage
            .write(&uri, content, content_hash)
            .await
            .map_err(map_write_error)?;

        let new_hash = ContentHash::from_content(content);
        let file_path = ensure_markdown_extension(&uri);
        return success_response(&uri, file_path, new_hash.as_str(), content.len());
    };

    match content_hash {
        None => {
            // Section create: note content read fresh if it exists, empty
            // otherwise (a brand-new note collapses into the same path).
            let current_content = if exists {
                storage
                    .read(&uri)
                    .await
                    .map_err(|e| {
                        ErrorData::internal_error(format!("Failed to read note: {}", e), None)
                    })?
                    .0
            } else {
                String::new()
            };

            let resolved =
                create_section(&current_content, section_path, content).map_err(|e| match e {
                    SectionCreateError::AlreadyExists { path } => ErrorData::invalid_params(
                        format!(
                            "Section already exists: {}. Read it first to get its content_hash, \
                             then include it in the write request.",
                            path
                        ),
                        None,
                    ),
                    SectionCreateError::Ambiguous { path, candidates } => {
                        ErrorData::invalid_params(
                            format!(
                                "Section path '{}' is ambiguous, matches: {}. Use a longer path \
                                 to disambiguate.",
                                path,
                                candidates.join(", ")
                            ),
                            None,
                        )
                    }
                    SectionCreateError::TooDeep { path } => ErrorData::invalid_params(
                        format!(
                            "Cannot create section nested this deep: '{}' would need a heading \
                             past level 6 (markdown's own limit). Split the note into more, \
                             shallower sections instead.",
                            path
                        ),
                        None,
                    ),
                })?;

            // TOCTOU guard: if the note already existed, CAS on the hash of
            // the content just read. A brand-new note has no prior state to
            // guard against - storage.write's own None-expected-hash path
            // still fails if something raced to create it first.
            let expected_hash = exists.then(|| {
                ContentHash::from_content(&current_content)
                    .as_str()
                    .to_string()
            });
            storage
                .write(&uri, &resolved.full_content, expected_hash.as_deref())
                .await
                .map_err(map_write_error)?;

            let file_path = ensure_markdown_extension(&uri);
            let new_section_hash = ContentHash::from_content(&resolved.section_content);
            success_response(
                &uri,
                file_path,
                new_section_hash.as_str(),
                resolved.section_content.len(),
            )
        }
        Some(hash) => {
            // Section edit: unchanged from `content_hash: Some, section: None`'s TOCTOU shape,
            // just narrowed to a section's range.
            if !exists {
                return Err(ErrorData::invalid_params(
                    format!("Note does not exist: {}", note),
                    None,
                ));
            }

            let (current_content, _) = storage.read(&uri).await.map_err(|e| {
                ErrorData::internal_error(format!("Failed to read note: {}", e), None)
            })?;

            let resolved = resolve_section_for_write(&current_content, section_path, hash)
                .map_err(|e| match e {
                    SectionWriteError::NotFound { path } => ErrorData::invalid_params(
                        format!(
                            "Section not found: {}. Use the outline tool to see available sections.",
                            path
                        ),
                        None,
                    ),
                    SectionWriteError::Ambiguous { path, candidates } => {
                        ErrorData::invalid_params(
                            format!(
                                "Section path '{}' is ambiguous, matches: {}. Use a longer path \
                                 to disambiguate.",
                                path,
                                candidates.join(", ")
                            ),
                            None,
                        )
                    }
                    SectionWriteError::HashMismatch { .. } => {
                        ErrorData::invalid_params(MODIFIED_SINCE_READ.to_string(), None)
                    }
                })?;

            let new_full_content = splice_section(
                &current_content,
                resolved.start_line,
                resolved.end_line,
                content,
            );

            // Whole-file hash captured from the same read used to resolve
            // the section, so a mismatch here can only mean a genuinely
            // concurrent out-of-band change to a different part of the file
            // - the target section's own drift was already caught above.
            let whole_file_hash = ContentHash::from_content(&current_content);
            storage
                .write(&uri, &new_full_content, Some(whole_file_hash.as_str()))
                .await
                .map_err(map_write_error)?;

            let file_path = ensure_markdown_extension(&uri);
            let new_section_hash = ContentHash::from_content(content);
            success_response(&uri, file_path, new_section_hash.as_str(), content.len())
        }
    }
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
        path: String,
        content_hash: String,
        bytes_written: usize,
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
    async fn test_write_new_note() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "Hello, world!",
            None, // No hash for new file
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.uri, "memory:test");
        assert_eq!(response.path, "test.md");
        assert_eq!(response.bytes_written, 13);
        assert!(!response.content_hash.is_empty());

        // Verify file was created
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Hello, world!");
    }

    #[tokio::test]
    async fn test_write_existing_requires_content_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create existing note
        fs::write(temp_dir.path().join("test.md"), "Existing content")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        // Try to write without content_hash
        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "New content",
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note already exists"));
        assert!(err.message.contains("Read it first"));
    }

    #[tokio::test]
    async fn test_write_existing_with_correct_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create existing note
        let original_content = "Version 1";
        fs::write(temp_dir.path().join("test.md"), original_content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        // Get the correct hash
        let correct_hash = ContentHash::from_content(original_content);

        // Write with correct hash should succeed
        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "Version 2",
            Some(correct_hash.as_str()),
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.uri, "memory:test");

        // Verify content changed
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Version 2");
    }

    #[tokio::test]
    async fn test_write_existing_with_wrong_hash() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create existing note
        fs::write(temp_dir.path().join("test.md"), "Existing content")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        // Try to write with wrong hash
        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "New content",
            Some("wrong_hash"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note modified since last read"));
    }

    #[tokio::test]
    async fn test_write_returns_new_hash_for_chained_writes() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // First write - creates new file
        let result1 = execute(temp_dir.path(), &storage, &graph, "test", "Version 1", None)
            .await
            .expect("should succeed");

        let response1 = parse_response(&result1);

        // Second write - uses hash from first write
        let result2 = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "Version 2",
            Some(&response1.content_hash),
        )
        .await
        .expect("should succeed");

        let response2 = parse_response(&result2);

        // Hashes should be different
        assert_ne!(response1.content_hash, response2.content_hash);

        // Third write - uses hash from second write
        let result3 = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "Version 3",
            Some(&response2.content_hash),
        )
        .await
        .expect("should succeed");

        let response3 = parse_response(&result3);
        assert_ne!(response2.content_hash, response3.content_hash);

        // Verify final content
        let content = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(content, "Version 3");
    }

    #[tokio::test]
    async fn test_write_to_subdirectory() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Create subdirectory
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "knowledge/test",
            "Content",
            None,
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(response.uri, "memory:knowledge/test");
        assert_eq!(response.path, "knowledge/test.md");

        // Verify file was created
        assert!(temp_dir.path().join("knowledge/test.md").exists());
    }

    #[tokio::test]
    async fn test_write_fails_if_parent_missing() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "missing/parent/test",
            "Content",
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Parent directory doesn't exist"));
    }

    #[tokio::test]
    async fn test_write_with_hash_for_nonexistent_file_fails() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Try to write with a hash when file doesn't exist
        let result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "new_note",
            "Content",
            Some("some_hash"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note does not exist"));
    }

    // Integration tests - test the actual ReadNote→WriteNote flow

    #[tokio::test]
    async fn test_read_then_write_flow() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        // Create note in subdirectory
        fs::create_dir(temp_dir.path().join("knowledge"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("knowledge/My Note.md"), "Version 1")
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
        assert_eq!(read_json["content"].as_str().unwrap(), "1\tVersion 1");

        // Step 2: WriteNote with hash from read
        let write_result = execute(
            temp_dir.path(),
            &storage,
            &graph,
            "My Note",
            "Version 2",
            Some(content_hash),
        )
        .await
        .expect("WriteNote should succeed");

        let response = parse_response(&write_result);
        assert_eq!(response.uri, "memory:knowledge/My Note");

        // Verify the file was actually modified
        let content = fs::read_to_string(temp_dir.path().join("knowledge/My Note.md"))
            .await
            .unwrap();
        assert_eq!(content, "Version 2");
    }

    // -- Section-scoped write tests (Commit 2) -------------------------------

    #[tokio::test]
    async fn test_create_section_on_brand_new_note() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "new_note",
            "first entry",
            None,
            Some("Daily Log"),
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(
            response.content_hash,
            ContentHash::from_content("# Daily Log\nfirst entry").as_str()
        );

        let written = fs::read_to_string(temp_dir.path().join("new_note.md"))
            .await
            .unwrap();
        assert_eq!(written, "# Daily Log\nfirst entry");
    }

    #[tokio::test]
    async fn test_create_section_single_level_on_existing_note() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# Top\nintro")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "middle body",
            None,
            Some("Top > Middle"),
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(
            response.content_hash,
            ContentHash::from_content("## Middle\nmiddle body").as_str()
        );

        let written = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(written, "# Top\nintro\n## Middle\nmiddle body");
    }

    #[tokio::test]
    async fn test_create_section_nested_ancestors_on_existing_note() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# Top\nintro")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "leaf body",
            None,
            Some("Top > Middle > Leaf"),
        )
        .await
        .expect("should succeed");
        let _ = parse_response(&result);

        let written = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(written, "# Top\nintro\n## Middle\n### Leaf\nleaf body");
    }

    #[tokio::test]
    async fn test_create_section_when_already_exists_errors() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# Top\n## Middle\nbody")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "replacement",
            None,
            Some("Top > Middle"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("already exists"));
        assert!(err.message.contains("Read it first"));

        // The note must be untouched by the rejected create.
        let written = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(written, "# Top\n## Middle\nbody");
    }

    #[tokio::test]
    async fn test_edit_existing_section_through_write_note() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        let content = "# Top\nintro\n## Middle\nold body\n# Top Two\nmore";
        fs::write(temp_dir.path().join("test.md"), content)
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let section_hash = ContentHash::from_content("## Middle\nold body");

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "## Middle\nnew body",
            Some(section_hash.as_str()),
            Some("Top > Middle"),
        )
        .await
        .expect("should succeed");

        let response = parse_response(&result);
        assert_eq!(
            response.content_hash,
            ContentHash::from_content("## Middle\nnew body").as_str()
        );

        let written = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(
            written,
            "# Top\nintro\n## Middle\nnew body\n# Top Two\nmore"
        );
    }

    #[tokio::test]
    async fn test_edit_section_with_stale_hash_rejected() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# A\noriginal")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let stale_hash = ContentHash::from_content("# A\nsomething else entirely");

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "# A\nreplaced",
            Some(stale_hash.as_str()),
            Some("A"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("modified since last read"));
    }

    #[tokio::test]
    async fn test_edit_section_not_found_maps_to_teaching_error() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(temp_dir.path().join("test.md"), "# A\nbody")
            .await
            .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "# Nonexistent\nreplacement",
            Some("irrelevant_hash"),
            Some("Nonexistent"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Section not found: Nonexistent"));
        assert!(err.message.contains("outline"));
    }

    #[tokio::test]
    async fn test_edit_section_ambiguous_path_maps_to_teaching_error() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "# Notes\nfirst\n# Notes\nsecond",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "# Notes\nreplacement",
            Some("irrelevant_hash"),
            Some("Notes"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("ambiguous"));
        assert!(err.message.contains("matches: Notes, Notes"));
        assert!(err.message.contains("disambiguate"));
    }

    #[tokio::test]
    async fn test_create_section_ambiguous_existing_path_maps_to_teaching_error() {
        let (temp_dir, storage, mut graph) = create_test_env().await;

        fs::write(
            temp_dir.path().join("test.md"),
            "# Notes\nfirst\n# Notes\nsecond",
        )
        .await
        .unwrap();
        graph.update_note("test", PathBuf::from("test.md"), HashSet::new());

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "new body",
            None,
            Some("Notes"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("ambiguous"));
        assert!(err.message.contains("matches: Notes, Notes"));
        assert!(err.message.contains("disambiguate"));
    }

    #[tokio::test]
    async fn test_edit_section_note_does_not_exist_errors() {
        let (temp_dir, storage, graph) = create_test_env().await;

        let result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "nonexistent",
            "# A\nbody",
            Some("some_hash"),
            Some("A"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Note does not exist"));
    }

    #[tokio::test]
    async fn test_chained_create_then_edit_via_content_hash() {
        let (temp_dir, storage, graph) = create_test_env().await;

        // Create the section.
        let create_result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "first body",
            None,
            Some("Notes"),
        )
        .await
        .expect("create should succeed");
        let created = parse_response(&create_result);

        // Use the returned hash to edit it in place.
        let edit_result = execute_scoped(
            temp_dir.path(),
            &storage,
            &graph,
            "test",
            "# Notes\nsecond body",
            Some(&created.content_hash),
            Some("Notes"),
        )
        .await
        .expect("chained edit should succeed");
        let _ = parse_response(&edit_result);

        let written = fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(written, "# Notes\nsecond body");
    }
}
