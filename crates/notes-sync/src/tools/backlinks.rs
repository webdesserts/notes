//! Backlinks tool - list a note's incoming and outgoing wiki-links.

use notes_core::note_ref::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use tokio::sync::RwLock;

use super::common::{resolve_backlinks, resolve_forward_links, resolve_note_uri};
use crate::graph::GraphIndex;
use crate::storage::Storage;

/// Execute the Backlinks tool.
///
/// Resolves `note` with graph-aware resolution and lists the notes that link
/// to it (backlinks) and the notes it links to (outgoing links), both as URIs.
pub async fn execute<S: Storage>(
    storage: &S,
    graph: &RwLock<GraphIndex>,
    note: &str,
) -> Result<CallToolResult, ErrorData> {
    let graph_read = graph.read().await;
    let (uri, exists) = resolve_note_uri(storage, &graph_read, note)
        .await
        .map_err(|e| ErrorData::internal_error(format!("Failed to resolve note: {}", e), None))?;

    if !exists {
        return Err(ErrorData::invalid_params(
            format!("Note not found: {}", note),
            None,
        ));
    }

    let name = uri.rsplit('/').next().unwrap_or(&uri);
    let path = ensure_markdown_extension(&uri);
    let mut incoming = resolve_backlinks(&graph_read, name);
    let self_uri = format!("memory:{}", uri);
    incoming.retain(|u| u != &self_uri);
    incoming.sort();
    let mut outgoing = resolve_forward_links(&graph_read, &path);
    outgoing.sort();

    let list = |items: &[String]| {
        if items.is_empty() {
            "(none)".to_string()
        } else {
            items
                .iter()
                .map(|u| format!("- {}", u))
                .collect::<Vec<_>>()
                .join("\n")
        }
    };

    let text = format!(
        "# Links for memory:{}\n\n## Backlinks ({})\n{}\n\n## Outgoing links ({})\n{}",
        uri,
        incoming.len(),
        list(&incoming),
        outgoing.len(),
        list(&outgoing)
    );

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FileStorage;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    async fn add(temp: &TempDir, graph: &RwLock<GraphIndex>, rel: &str, content: &str) {
        let full = temp.path().join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(&full, content).await.unwrap();
        let name = std::path::Path::new(rel)
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();
        graph.write().await.update_note(
            &name,
            PathBuf::from(rel),
            notes_core::wiki_links::extract_linked_notes(content)
                .into_iter()
                .collect(),
        );
    }

    fn text(result: &CallToolResult) -> String {
        result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone()
    }

    #[tokio::test]
    async fn test_backlinks_lists_incoming_and_outgoing() {
        let temp = TempDir::new().unwrap();
        let storage = FileStorage::new(temp.path().to_path_buf());
        let graph = RwLock::new(GraphIndex::new());
        add(&temp, &graph, "knowledge/Target.md", "Points at [[Other]]").await;
        add(&temp, &graph, "Other.md", "Plain").await;
        add(&temp, &graph, "projects/A.md", "See [[Target]]").await;
        add(&temp, &graph, "B.md", "See [[knowledge/Target|alias]]").await;

        let out = text(&execute(&storage, &graph, "Target").await.expect("ok"));
        assert!(out.contains("# Links for memory:knowledge/Target"), "{out}");
        assert!(out.contains("## Backlinks (2)"), "{out}");
        assert!(out.contains("- memory:projects/A"), "{out}");
        assert!(out.contains("- memory:B"), "{out}");
        assert!(out.contains("## Outgoing links (1)"), "{out}");
        assert!(out.contains("- memory:Other"), "{out}");
    }

    #[tokio::test]
    async fn test_backlinks_missing_note_errors() {
        let temp = TempDir::new().unwrap();
        let storage = FileStorage::new(temp.path().to_path_buf());
        let graph = RwLock::new(GraphIndex::new());
        assert!(execute(&storage, &graph, "Nope").await.is_err());
    }
}
