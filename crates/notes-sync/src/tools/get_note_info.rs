use obsidian_fs::{
    NoteRef, ensure_markdown_extension, generate_search_paths, normalize_note_reference,
};
use rmcp::model::{CallToolResult, Content, ErrorData};
use std::path::Path;

use crate::graph::GraphIndex;
use crate::tools::common::{
    format_frontmatter_summary, format_links_summary, read_frontmatter_keys, resolve_backlinks,
    resolve_forward_links,
};

/// Resolve a note reference to a file path, searching the vault if needed.
///
/// Returns (resolved_path_without_ext, exists)
async fn resolve_note_to_path(
    vault_path: &Path,
    note_ref: &NoteRef,
    graph: &GraphIndex,
) -> (String, bool) {
    // First, check if the note is in the graph index
    if let Some(graph_path) = graph.get_path(&note_ref.name) {
        // Found in graph - convert PathBuf to string without .md extension
        let path_str = graph_path.to_string_lossy();
        let path_without_ext = path_str.strip_suffix(".md").unwrap_or(&path_str);
        return (path_without_ext.to_string(), true);
    }

    // Not in graph - try to find file on disk
    // If the reference includes a path (e.g., "knowledge/Note"), try that first
    if note_ref.path.contains('/') {
        let file_path = vault_path.join(ensure_markdown_extension(&note_ref.path));
        if file_path.exists() {
            return (note_ref.path.clone(), true);
        }
    }

    // Generate search paths and try each one
    let search_paths = generate_search_paths(&note_ref.name);
    for search_path in &search_paths {
        let file_path = vault_path.join(ensure_markdown_extension(search_path));
        if file_path.exists() {
            return (search_path.clone(), true);
        }
    }

    // Not found anywhere - return the original path
    (note_ref.path.clone(), false)
}

/// Execute the GetNote tool
pub async fn execute(
    vault_path: &Path,
    vault_name: &str,
    graph: &GraphIndex,
    note: &str,
) -> Result<CallToolResult, ErrorData> {
    // Normalize the note reference
    let note_ref = normalize_note_reference(note);
    let note_name = note_ref.name.clone();

    // Resolve to actual path
    let (resolved_path, exists) = resolve_note_to_path(vault_path, &note_ref, graph).await;

    // Build URIs
    let file_path = vault_path
        .join(ensure_markdown_extension(&resolved_path))
        .to_string_lossy()
        .to_string();
    let memory_uri = format!("memory:{}", resolved_path);
    let obsidian_uri = format!(
        "obsidian://open?vault={}&file={}",
        urlencoding::encode(vault_name),
        urlencoding::encode(&resolved_path)
    );

    if !exists {
        // Note doesn't exist - return helpful message
        let text = format!(
            "Note not found: {}\n\n\
             This note doesn't exist yet. You can create it at:\n\
             - File path: {}\n\
             - Memory URI: {}\n\
             - Obsidian URI: {}\n\n\
             Use WriteNote tool to create this note.",
            note_name, file_path, memory_uri, obsidian_uri
        );
        return Ok(CallToolResult::success(vec![Content::text(text)]));
    }

    // Note exists - get links and frontmatter using shared helpers
    let path_with_ext = ensure_markdown_extension(&resolved_path);
    let forward_links = resolve_forward_links(graph, &path_with_ext);
    let backlinks = resolve_backlinks(graph, &note_name);
    let frontmatter_keys = read_frontmatter_keys(&file_path).await;

    // Build response text using shared formatters
    let (links_summary, backlinks_summary) = format_links_summary(&forward_links, &backlinks);
    let frontmatter_summary = format_frontmatter_summary(&frontmatter_keys);

    let text = format!(
        "Note: {}\n\
         Path: {}\n\
         File: {}\n\
         Memory URI: {}\n\
         Obsidian URI: {}{}{}{}\n\n\
         Use ReadNote tool to view content.",
        note_name,
        resolved_path,
        file_path,
        memory_uri,
        obsidian_uri,
        links_summary,
        backlinks_summary,
        frontmatter_summary
    );

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;
    use tokio::fs;

    async fn create_test_vault() -> (TempDir, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        // Create some test notes
        fs::create_dir_all(vault_path.join("knowledge"))
            .await
            .unwrap();

        // Note A links to Note B
        fs::write(
            vault_path.join("knowledge/Note A.md"),
            "---\ntype: test\n---\n\nContent linking to [[Note B]]",
        )
        .await
        .unwrap();

        // Note B
        fs::write(
            vault_path.join("knowledge/Note B.md"),
            "---\ntags: [one, two]\n---\n\nNote B content",
        )
        .await
        .unwrap();

        // Build graph
        let mut graph = GraphIndex::new();
        graph.update_note(
            "Note A",
            "knowledge/Note A.md".into(),
            ["Note B".to_string()].into_iter().collect(),
        );
        graph.update_note("Note B", "knowledge/Note B.md".into(), HashSet::new());

        (temp_dir, graph)
    }

    #[tokio::test]
    async fn test_get_existing_note() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph, "Note A")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Note: Note A"));
        assert!(text.contains("knowledge/Note A"));
        assert!(text.contains("memory:knowledge/Note A"));
    }

    #[tokio::test]
    async fn test_get_nonexistent_note() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph, "Nonexistent Note")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Note not found"));
        assert!(text.contains("Nonexistent Note"));
    }

    #[tokio::test]
    async fn test_get_note_with_links() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph, "Note A")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Should show forward links
        assert!(text.contains("Links to:"));
        assert!(text.contains("Note B"));
    }

    #[tokio::test]
    async fn test_get_note_with_backlinks() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph, "Note B")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Should show backlinks
        assert!(text.contains("Linked from:"));
        assert!(text.contains("Note A"));
    }

    /// Builds a vault through the real content -> extractor -> graph path
    /// (`GraphIndex::initialize`), unlike `create_test_vault()`'s hand-typed
    /// `update_note` calls which bypass `extract_linked_notes` entirely and so
    /// can't catch bugs in the extractor itself.
    async fn create_dotted_link_vault() -> (TempDir, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        fs::write(vault_path.join("llama.cpp.md"), "Notes on llama.cpp")
            .await
            .unwrap();

        fs::write(
            vault_path.join("Note About Llama.md"),
            "See [[llama.cpp]] for details",
        )
        .await
        .unwrap();

        let mut graph = GraphIndex::new();
        graph.initialize(vault_path).await.unwrap();

        (temp_dir, graph)
    }

    #[tokio::test]
    async fn test_get_note_with_dotted_name_shows_real_backlinks() {
        let (temp_dir, graph) = create_dotted_link_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph, "llama.cpp")
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // The dotted note indexes under its full name and shows the real backlink.
        assert!(text.contains("Linked from:"));
        assert!(text.contains("Note About Llama"));

        // No phantom "llama" entry (the extension-truncated name) exists anywhere
        // in the graph.
        assert!(graph.get_backlinks("llama").is_none());
    }

    #[tokio::test]
    async fn test_normalizes_note_reference() {
        let (temp_dir, graph) = create_test_vault().await;

        // Test with memory: URI
        let result = execute(
            temp_dir.path(),
            "test-vault",
            &graph,
            "memory:knowledge/Note A",
        )
        .await
        .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        assert!(text.contains("Note: Note A"));
    }
}
