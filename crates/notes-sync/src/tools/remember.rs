//! Remember Tool - Load all session context files in a single call
//!
//! Returns Log.md, Working Memory.md, current weekly note, and discovered project notes.
//! Automatically discovers projects based on git remotes and directory names.
//! Use this at the start of every session to get complete context about recent work,
//! current focus, this week's activity, and project context.

use std::path::Path;

use rmcp::model::{CallToolResult, Content, ErrorData, ResourceContents};

use crate::graph::GraphIndex;
use crate::projects::{DiscoveryResult, discover_projects, generate_discovery_status_message};
use crate::tools::get_weekly_note_info;

/// Execute the Remember tool
pub async fn execute(
    vault_path: &Path,
    graph_index: &GraphIndex,
    cwd: Option<&Path>,
) -> Result<CallToolResult, ErrorData> {
    // Define paths to all context files
    let log_path = vault_path.join("Log.md");
    let working_memory_path = vault_path.join("Working Memory.md");

    // Get weekly note path
    let (weekly_note_uri, weekly_note_path) = get_weekly_note_path(vault_path);

    // Discover projects if CWD was provided
    let discovery_result = cwd.map(|cwd| discover_projects(cwd, graph_index, vault_path));

    // Read all context files
    let log_content = tokio::fs::read_to_string(&log_path).await.ok();
    let working_memory_content = tokio::fs::read_to_string(&working_memory_path).await.ok();
    let weekly_note_content = tokio::fs::read_to_string(&weekly_note_path).await.ok();

    // Read strict match project notes
    let mut project_contents = Vec::new();
    if let Some(ref result) = discovery_result {
        for m in &result.strict_matches {
            if let Ok(content) = tokio::fs::read_to_string(&m.metadata.file_path).await {
                project_contents.push((
                    m.metadata.file_path.clone(),
                    m.metadata.name.clone(),
                    content,
                ));
            }
        }
    }

    // Build content blocks array - one resource per file
    let mut content_blocks: Vec<Content> = Vec::new();

    if let Some(content) = log_content {
        content_blocks.push(Content::resource(ResourceContents::TextResourceContents {
            uri: format!("file://{}", log_path.display()),
            mime_type: Some("text/markdown".into()),
            text: content,
            meta: None,
        }));
    }

    if let Some(content) = working_memory_content {
        content_blocks.push(Content::resource(ResourceContents::TextResourceContents {
            uri: format!("file://{}", working_memory_path.display()),
            mime_type: Some("text/markdown".into()),
            text: content,
            meta: None,
        }));
    }

    if let Some(content) = weekly_note_content {
        content_blocks.push(Content::resource(ResourceContents::TextResourceContents {
            uri: weekly_note_uri,
            mime_type: Some("text/markdown".into()),
            text: content,
            meta: None,
        }));
    }

    // Add strictly matched project notes
    for (file_path, _name, content) in project_contents {
        content_blocks.push(Content::resource(ResourceContents::TextResourceContents {
            uri: format!("file://{}", file_path.display()),
            mime_type: Some("text/markdown".into()),
            text: content,
            meta: None,
        }));
    }

    // Generate project discovery status message
    let project_status = match (&discovery_result, cwd) {
        (Some(result), Some(cwd)) => generate_discovery_status_message(result, cwd),
        _ => "Project discovery skipped (no cwd provided).".to_string(),
    };

    // Add project status as text content
    content_blocks.push(Content::text(project_status));

    let structured = match &discovery_result {
        Some(result) => build_structured_content(result),
        None => serde_json::json!({
            "projectsFound": 0,
            "projectDisconnects": 0,
            "projectSuggestions": 0,
        }),
    };

    Ok(CallToolResult {
        content: content_blocks,
        is_error: None,
        meta: None,
        structured_content: Some(structured),
    })
}

/// Get the weekly note URI and file path
fn get_weekly_note_path(vault_path: &Path) -> (String, std::path::PathBuf) {
    let (iso_week_date, _) = get_weekly_note_info::get_current_week_info();

    // Build file path directly (simpler than parsing URI)
    let file_path = vault_path.join(format!("journal/{}.md", iso_week_date));
    let weekly_note_uri = format!("file://{}", file_path.display());

    (weekly_note_uri, file_path)
}

/// Build structured content for the response
fn build_structured_content(discovery_result: &DiscoveryResult) -> serde_json::Value {
    serde_json::json!({
        "projectsFound": discovery_result.strict_matches.len(),
        "projectDisconnects": discovery_result.loose_matches.len(),
        "projectSuggestions": discovery_result.suggestions.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn create_test_vault() -> (TempDir, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        // Create Log.md
        std::fs::write(
            vault_path.join("Log.md"),
            "## 2025-W01-1 (Mon)\n\n- 9:00 AM – Started work\n",
        )
        .unwrap();

        // Create Working Memory.md
        std::fs::write(
            vault_path.join("Working Memory.md"),
            "### Active\n\nSome notes here\n",
        )
        .unwrap();

        // Create journal folder and weekly note
        std::fs::create_dir_all(vault_path.join("journal")).unwrap();
        let (iso_week_date, _) = get_weekly_note_info::get_current_week_info();
        std::fs::write(
            vault_path.join(format!("journal/{}.md", iso_week_date.to_lowercase())),
            "# Week Notes\n\nThis week's journal\n",
        )
        .unwrap();

        // Create projects folder with a test project
        std::fs::create_dir_all(vault_path.join("projects")).unwrap();
        std::fs::write(
            vault_path.join("projects/Test Project.md"),
            "---\ntype: project\nremotes:\n  - git@github.com:user/test.git\nslug: test\n---\n\nTest project notes\n",
        )
        .unwrap();

        // Create graph index with the project
        let mut graph = GraphIndex::new();
        graph.update_note(
            "Test Project",
            std::path::PathBuf::from("projects/Test Project.md"),
            HashSet::new(),
        );

        (temp_dir, graph)
    }

    #[tokio::test]
    async fn test_remember_loads_context_files() {
        let (temp_dir, graph) = create_test_vault();
        let vault_path = temp_dir.path();

        // Use a non-matching CWD so we don't trigger project discovery
        let result = execute(vault_path, &graph, Some(Path::new("/tmp")))
            .await
            .unwrap();

        // Should have at least Log, Working Memory, Weekly Note, and status message
        assert!(result.content.len() >= 3);

        // Check that we have resource blocks
        let resource_count = result
            .content
            .iter()
            .filter(|c| c.raw.as_resource().is_some())
            .count();
        assert!(
            resource_count >= 3,
            "Expected at least 3 resources, got {}",
            resource_count
        );
    }

    #[tokio::test]
    async fn test_remember_discovers_projects() {
        let (temp_dir, graph) = create_test_vault();
        let vault_path = temp_dir.path();

        // Create a test directory with matching git remote
        let test_cwd = temp_dir.path().join("test-project");
        std::fs::create_dir_all(&test_cwd).unwrap();

        // Initialize git repo with matching remote
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(&test_cwd)
            .output()
            .ok();
        std::process::Command::new("git")
            .args(["remote", "add", "origin", "git@github.com:user/test.git"])
            .current_dir(&test_cwd)
            .output()
            .ok();

        let result = execute(vault_path, &graph, Some(&test_cwd)).await.unwrap();

        // Check structured content shows project found
        let structured = result.structured_content.unwrap();
        assert_eq!(structured["projectsFound"], 1);
    }

    #[tokio::test]
    async fn test_remember_handles_missing_files() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let graph = GraphIndex::new();

        // Empty vault - no files exist
        let result = execute(vault_path, &graph, Some(Path::new("/tmp")))
            .await
            .unwrap();

        // Should still succeed with just the status message
        assert!(!result.content.is_empty());

        // Check that we have at least a text content (status message)
        let text_count = result
            .content
            .iter()
            .filter(|c| c.raw.as_text().is_some())
            .count();
        assert!(text_count >= 1);
    }

    #[tokio::test]
    async fn test_remember_without_cwd_skips_discovery() {
        let (temp_dir, graph) = create_test_vault();
        let vault_path = temp_dir.path();

        // No CWD provided — should still load context files but skip project discovery
        let result = execute(vault_path, &graph, None).await.unwrap();

        // Should have Log, Working Memory, Weekly Note, and status message
        let resource_count = result
            .content
            .iter()
            .filter(|c| c.raw.as_resource().is_some())
            .count();
        assert!(
            resource_count >= 3,
            "Expected at least 3 resources, got {}",
            resource_count
        );

        // Structured content should show no projects found
        let structured = result.structured_content.unwrap();
        assert_eq!(structured["projectsFound"], 0);

        // Status message should indicate discovery was skipped
        let text_content: Vec<_> = result
            .content
            .iter()
            .filter_map(|c| c.raw.as_text().map(|t| t.text.as_str()))
            .collect();
        assert!(text_content.iter().any(|t| t.contains("skipped")));
    }
}
