//! Note references and vault-relative paths: note lookup, normalization,
//! search path generation, and path validation. Pure functions with no I/O.

use serde::{Deserialize, Serialize};

/// Common search paths for note lookup (relative to vault root)
pub const COMMON_SEARCH_PATHS: &[&str] = &[
    "", // Root
    "knowledge",
    "journal",
    "projects",
];

/// Priority categories for path resolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PathPriority {
    Root = 0,
    Knowledge = 1,
    Journal = 2,
    Projects = 3,
    Other = 4,
}

fn get_priority(path: &str) -> PathPriority {
    if !path.contains('/') {
        PathPriority::Root
    } else if path.starts_with("knowledge/") {
        PathPriority::Knowledge
    } else if path.starts_with("journal/") {
        PathPriority::Journal
    } else if path.starts_with("projects/") {
        PathPriority::Projects
    } else {
        PathPriority::Other
    }
}

/// Resolve a note path from available options using priority order.
///
/// Priority: root → knowledge/ → journal/ → projects/ → others
///
/// Returns the best matching path, or None if no paths provided.
pub fn resolve_note_path(available_paths: &[&str]) -> Option<String> {
    if available_paths.is_empty() {
        return None;
    }
    if available_paths.len() == 1 {
        return Some(available_paths[0].to_string());
    }

    let mut sorted = available_paths.to_vec();
    sorted.sort_by_key(|p| get_priority(p));
    sorted.first().map(|s| s.to_string())
}

/// Generate search paths for a note name.
///
/// Returns an array of paths to try (without .md extension).
pub fn generate_search_paths(note_name: &str) -> Vec<String> {
    let mut paths = Vec::new();

    for folder in COMMON_SEARCH_PATHS {
        if folder.is_empty() {
            paths.push(note_name.to_string());
        } else {
            paths.push(format!("{}/{}", folder, note_name));
        }
    }

    paths
}

/// A normalized note reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoteRef {
    /// The path without extension: "knowledge/Note"
    pub path: String,
    /// Just the note name: "Note"
    pub name: String,
}

/// Normalize a note reference (strip memory: URI scheme, [[brackets]], and .md extension)
///
/// Accepts:
/// - Plain name: "Note"
/// - Path: "knowledge/Note"
/// - Memory URI: "memory:knowledge/Note"
/// - Wiki link: "[[knowledge/Note]]"
/// - With .md: "knowledge/Note.md"
pub fn normalize_note_reference(note_ref: &str) -> NoteRef {
    let mut normalized = note_ref.trim();

    // Strip [[wiki link]] brackets if present
    if normalized.starts_with("[[") && normalized.ends_with("]]") {
        normalized = &normalized[2..normalized.len() - 2];
    }

    // Strip memory: URI scheme
    if let Some(stripped) = normalized.strip_prefix("memory:") {
        normalized = stripped;
    }

    // Strip .md extension if present
    let path = if let Some(stripped) = normalized.strip_suffix(".md") {
        stripped
    } else {
        normalized
    };

    // Extract just the note name (last path component)
    let name = path.rsplit('/').next().unwrap_or(path).to_string();

    NoteRef {
        path: path.to_string(),
        name,
    }
}

/// Validate that a relative path is safe (no directory traversal)
pub fn validate_relative_path(path: &str) -> Result<String, PathValidationError> {
    // Remove leading slash if present
    let clean_path = path.strip_prefix('/').unwrap_or(path);

    // Check for directory traversal attempts
    if clean_path.contains("..") {
        return Err(PathValidationError::DirectoryTraversal);
    }

    // Check for absolute paths (shouldn't happen after stripping leading slash, but be safe)
    if clean_path.starts_with('/') {
        return Err(PathValidationError::AbsolutePath);
    }

    Ok(clean_path.to_string())
}

/// Ensure .md extension on note paths
pub fn ensure_markdown_extension(note_path: &str) -> String {
    if note_path.ends_with(".md") {
        note_path.to_string()
    } else {
        format!("{}.md", note_path)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathValidationError {
    DirectoryTraversal,
    AbsolutePath,
}

impl std::fmt::Display for PathValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathValidationError::DirectoryTraversal => {
                write!(f, "Path contains directory traversal")
            }
            PathValidationError::AbsolutePath => write!(f, "Path is absolute"),
        }
    }
}

impl std::error::Error for PathValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    // resolveNotePath tests
    #[test]
    fn resolve_returns_none_for_empty_paths() {
        let result = resolve_note_path(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn resolve_returns_only_path_when_one_option() {
        let result = resolve_note_path(&["knowledge/Test"]);
        assert_eq!(result, Some("knowledge/Test".to_string()));
    }

    #[test]
    fn resolve_prioritizes_root_level_notes() {
        let paths = vec!["other/Index", "Index", "knowledge/Index"];
        let result = resolve_note_path(&paths);
        assert_eq!(result, Some("Index".to_string()));
    }

    #[test]
    fn resolve_prioritizes_knowledge_over_journal() {
        let paths = vec!["journal/Note", "knowledge/Note"];
        let result = resolve_note_path(&paths);
        assert_eq!(result, Some("knowledge/Note".to_string()));
    }

    #[test]
    fn resolve_prioritizes_journal_over_other_folders() {
        let paths = vec!["other/Note", "journal/Note"];
        let result = resolve_note_path(&paths);
        assert_eq!(result, Some("journal/Note".to_string()));
    }

    // generateSearchPaths tests
    #[test]
    fn generate_common_search_paths() {
        let paths = generate_search_paths("Test");
        assert_eq!(
            paths,
            vec!["Test", "knowledge/Test", "journal/Test", "projects/Test"]
        );
    }

    // normalizeNoteReference tests
    #[test]
    fn normalize_strips_memory_prefix() {
        let result = normalize_note_reference("memory:knowledge/Note");
        assert_eq!(result.path, "knowledge/Note");
        assert_eq!(result.name, "Note");
    }

    #[test]
    fn normalize_strips_wiki_link_brackets() {
        let result = normalize_note_reference("[[knowledge/Note]]");
        assert_eq!(result.path, "knowledge/Note");
        assert_eq!(result.name, "Note");
    }

    #[test]
    fn normalize_strips_md_extension() {
        let result = normalize_note_reference("knowledge/Note.md");
        assert_eq!(result.path, "knowledge/Note");
        assert_eq!(result.name, "Note");
    }

    #[test]
    fn normalize_strips_both_prefix_and_extension() {
        let result = normalize_note_reference("memory:knowledge/Note.md");
        assert_eq!(result.path, "knowledge/Note");
    }

    #[test]
    fn normalize_handles_wiki_links_with_memory_uris() {
        let result = normalize_note_reference("[[memory:knowledge/Note]]");
        assert_eq!(result.path, "knowledge/Note");
    }

    #[test]
    fn normalize_returns_note_as_is_if_already_normalized() {
        let result = normalize_note_reference("knowledge/Note");
        assert_eq!(result.path, "knowledge/Note");
    }

    #[test]
    fn normalize_extracts_name_from_path() {
        let result = normalize_note_reference("knowledge/subfolder/Note");
        assert_eq!(result.name, "Note");
    }

    #[test]
    fn normalize_handles_root_level_note() {
        let result = normalize_note_reference("Note");
        assert_eq!(result.path, "Note");
        assert_eq!(result.name, "Note");
    }

    // validateRelativePath tests
    #[test]
    fn validate_rejects_directory_traversal() {
        let result = validate_relative_path("../secret");
        assert_eq!(result, Err(PathValidationError::DirectoryTraversal));
    }

    #[test]
    fn validate_strips_leading_slash() {
        let result = validate_relative_path("/knowledge/Note");
        assert_eq!(result, Ok("knowledge/Note".to_string()));
    }

    #[test]
    fn validate_accepts_normal_path() {
        let result = validate_relative_path("knowledge/Note");
        assert_eq!(result, Ok("knowledge/Note".to_string()));
    }

    // ensureMarkdownExtension tests
    #[test]
    fn ensure_adds_md_extension() {
        let result = ensure_markdown_extension("knowledge/Note");
        assert_eq!(result, "knowledge/Note.md");
    }

    #[test]
    fn ensure_keeps_existing_md_extension() {
        let result = ensure_markdown_extension("knowledge/Note.md");
        assert_eq!(result, "knowledge/Note.md");
    }
}
