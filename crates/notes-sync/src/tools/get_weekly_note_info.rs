use std::path::Path;

use chrono::{Datelike, IsoWeek, Local};
use obsidian_fs::ensure_markdown_extension;
use rmcp::model::{CallToolResult, Content, ErrorData};
use tokio::fs;

use crate::graph::GraphIndex;
use crate::tools::common::{
    format_frontmatter_summary, format_links_summary, read_frontmatter_keys, resolve_backlinks,
    resolve_forward_links,
};

/// Get the current ISO week date string and day name.
///
/// Returns (iso_week_date, day_name) where iso_week_date is like "2025-w01" (lowercase w).
pub fn get_current_week_info() -> (String, &'static str) {
    let now = Local::now();
    let iso_week: IsoWeek = now.iso_week();
    let week = iso_week.week();
    let year = iso_week.year();

    // Use lowercase 'w' to match vault naming convention
    let iso_week_date = format!("{}-w{:02}", year, week);

    let day_name = match now.weekday() {
        chrono::Weekday::Mon => "Monday",
        chrono::Weekday::Tue => "Tuesday",
        chrono::Weekday::Wed => "Wednesday",
        chrono::Weekday::Thu => "Thursday",
        chrono::Weekday::Fri => "Friday",
        chrono::Weekday::Sat => "Saturday",
        chrono::Weekday::Sun => "Sunday",
    };

    (iso_week_date, day_name)
}

/// Get metadata and graph connections for the current week's journal note.
///
/// Returns note location info (paths, URIs), links, backlinks, and frontmatter.
/// Works whether or not the note file exists yet.
pub async fn execute(
    vault_path: &Path,
    vault_name: &str,
    graph: &GraphIndex,
) -> Result<CallToolResult, ErrorData> {
    let (iso_week_date, current_day) = get_current_week_info();

    // Weekly note path format: journal/YYYY-wWW
    let note_path = format!("journal/{}", iso_week_date);
    let note_name = iso_week_date.clone();

    // Build URIs
    let file_path = vault_path
        .join(ensure_markdown_extension(&note_path))
        .to_string_lossy()
        .to_string();
    let memory_uri = format!("memory:{}", note_path);
    let obsidian_uri = format!(
        "obsidian://open?vault={}&file={}",
        urlencoding::encode(vault_name),
        urlencoding::encode(&note_path)
    );

    // Check if file exists
    let exists = fs::metadata(&file_path).await.is_ok();

    if !exists {
        // Note doesn't exist yet - return helpful message with paths
        let text = format!(
            "Weekly note: {} ({})\n\
             Status: Not created yet\n\n\
             Path: {}\n\
             File: {}\n\
             Memory URI: {}\n\
             Obsidian URI: {}\n\n\
             Use WriteNote tool to create this note.",
            note_name, current_day, note_path, file_path, memory_uri, obsidian_uri
        );
        return Ok(CallToolResult::success(vec![Content::text(text)]));
    }

    // Note exists - get links and frontmatter using shared helpers
    let path_with_ext = ensure_markdown_extension(&note_path);
    let forward_links = resolve_forward_links(graph, &path_with_ext);
    let backlinks = resolve_backlinks(graph, &note_name);
    let frontmatter_keys = read_frontmatter_keys(&file_path).await;

    // Build response text using shared formatters
    let (links_summary, backlinks_summary) = format_links_summary(&forward_links, &backlinks);
    let frontmatter_summary = format_frontmatter_summary(&frontmatter_keys);

    let text = format!(
        "Weekly note: {} ({})\n\
         Path: {}\n\
         File: {}\n\
         Memory URI: {}\n\
         Obsidian URI: {}{}{}{}\n\n\
         Use ReadNote tool to view content.",
        note_name,
        current_day,
        note_path,
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

    use tempfile::TempDir;

    async fn create_test_vault() -> (TempDir, GraphIndex) {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        // Create journal directory
        fs::create_dir_all(vault_path.join("journal"))
            .await
            .unwrap();

        // Build empty graph
        let graph = GraphIndex::new();

        (temp_dir, graph)
    }

    async fn create_test_vault_with_weekly_note() -> (TempDir, GraphIndex, String) {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        // Create journal directory
        fs::create_dir_all(vault_path.join("journal"))
            .await
            .unwrap();

        // Get current week info to create the right file
        let (iso_week_date, _) = get_current_week_info();
        let note_path = format!("journal/{}.md", iso_week_date);

        // Create weekly note with frontmatter and a link
        fs::write(
            vault_path.join(&note_path),
            "---\ntype: journal\n---\n\n# Weekly Journal\n\nWorking on [[Test Project]]",
        )
        .await
        .unwrap();

        // Build graph with the weekly note
        let mut graph = GraphIndex::new();
        graph.update_note(
            &iso_week_date,
            note_path.clone().into(),
            ["Test Project".to_string()].into_iter().collect(),
        );

        (temp_dir, graph, iso_week_date)
    }

    #[tokio::test]
    async fn test_get_weekly_note_info_not_exists() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph)
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Should indicate note doesn't exist
        assert!(text.contains("Not created yet"));
        // Should contain memory URI
        assert!(text.contains("memory:journal/"));
        // Should contain file path
        assert!(text.contains("journal/"));
        assert!(text.contains(".md"));
    }

    #[tokio::test]
    async fn test_get_weekly_note_info_exists() {
        let (temp_dir, graph, iso_week_date) = create_test_vault_with_weekly_note().await;

        let result = execute(temp_dir.path(), "test-vault", &graph)
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Should not indicate "Not created yet"
        assert!(!text.contains("Not created yet"));
        // Should contain the week date
        assert!(text.contains(&iso_week_date));
        // Should contain memory URI
        assert!(text.contains(&format!("memory:journal/{}", iso_week_date)));
        // Should show frontmatter
        assert!(text.contains("Frontmatter:"));
        assert!(text.contains("type"));
        // Should show links
        assert!(text.contains("Links to:"));
        assert!(text.contains("Test Project"));
    }

    #[tokio::test]
    async fn test_get_weekly_note_info_shows_day() {
        let (temp_dir, graph) = create_test_vault().await;

        let result = execute(temp_dir.path(), "test-vault", &graph)
            .await
            .expect("should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text")
            .text
            .clone();

        // Should show current day name
        let days = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ];
        assert!(days.iter().any(|day| text.contains(day)));
    }

    #[test]
    fn test_get_current_week_info_format() {
        let (iso_week_date, day_name) = get_current_week_info();

        // Format should be YYYY-wWW (lowercase w)
        assert!(iso_week_date.len() == 8);
        assert!(iso_week_date.contains("-w"));

        // Day name should be valid
        let valid_days = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ];
        assert!(valid_days.contains(&day_name));
    }
}
