//! Project discovery for linking working directories to project notes
//!
//! Algorithm:
//! 1. Crawl from CWD up to home directory
//! 2. For each directory, extract git remotes and directory name
//! 3. Search all project notes for strict matches (current remotes/slug)
//! 4. If no strict match, search for loose matches (old remotes/slugs)
//! 5. If no matches at all, find similar projects for suggestions
//! 6. Return all matches ordered by depth (closest first)

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value as JsonValue;

use crate::graph::GraphIndex;

use super::types::*;

/// Normalize git remote URL to a consistent format for comparison.
/// Handles both HTTPS and SSH formats, removes .git suffix, trailing slashes.
///
/// Examples:
/// - https://github.com/user/repo.git → github.com/user/repo
/// - git@github.com:user/repo.git → github.com/user/repo
/// - git@bitbucket.org:user/repo → bitbucket.org/user/repo
pub fn normalize_remote(remote: &str) -> String {
    let mut normalized = remote.trim().to_string();

    // Convert SSH format (git@host:path) to pseudo-URL (host/path)
    if normalized.starts_with("git@") {
        normalized = normalized
            .strip_prefix("git@")
            .unwrap_or(&normalized)
            .replace(':', "/");
    }

    // Remove protocol prefix (https://, http://, ssh://)
    if let Some(rest) = normalized.strip_prefix("https://") {
        normalized = rest.to_string();
    } else if let Some(rest) = normalized.strip_prefix("http://") {
        normalized = rest.to_string();
    } else if let Some(rest) = normalized.strip_prefix("ssh://") {
        normalized = rest.to_string();
    }

    // Remove .git suffix
    if let Some(stripped) = normalized.strip_suffix(".git") {
        normalized = stripped.to_string();
    }

    // Remove trailing slashes
    while normalized.ends_with('/') {
        normalized.pop();
    }

    // Lowercase for case-insensitive comparison
    normalized.to_lowercase()
}

/// Extract git remotes from a directory.
/// Returns empty Vec if not a git repo or if git command fails.
pub fn get_git_remotes(dir_path: &Path) -> Vec<String> {
    let output = match Command::new("git")
        .args(["remote", "-v"])
        .current_dir(dir_path)
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return Vec::new(),
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut remotes = HashSet::new();

    // Parse git remote output: "origin  git@github.com:user/repo.git (fetch)"
    for line in stdout.lines() {
        // Split on whitespace to get: ["origin", "url", "(fetch)"] or ["origin", "url", "(push)"]
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            remotes.insert(parts[1].to_string());
        }
    }

    remotes.into_iter().collect()
}

/// Crawl from CWD up to home directory, collecting directory info.
pub fn crawl_directories(cwd: &Path) -> Vec<DirectoryInfo> {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
    let mut directories = Vec::new();
    let mut current = cwd.to_path_buf();
    let mut depth = 0;

    loop {
        let name = current
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let git_remotes = get_git_remotes(&current);

        directories.push(DirectoryInfo {
            path: current.clone(),
            name,
            git_remotes,
            depth,
        });

        // Stop at home directory
        if current == home {
            break;
        }

        // Get parent, stop at filesystem root
        match current.parent() {
            Some(parent) if parent != current => {
                current = parent.to_path_buf();
                depth += 1;
            }
            _ => break,
        }
    }

    directories
}

/// Helper to extract a string array from a JSON value
fn json_to_string_vec(value: &JsonValue) -> Option<Vec<String>> {
    value.as_array().map(|arr| {
        arr.iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect()
    })
}

/// Load project metadata from a project note's frontmatter.
fn load_project_metadata(note_name: &str, file_path: &Path) -> Option<ProjectMetadata> {
    let content = std::fs::read_to_string(file_path).ok()?;
    let parsed = obsidian_fs::parse_frontmatter(&content);
    let frontmatter = parsed.frontmatter?;

    Some(ProjectMetadata {
        name: note_name.to_string(),
        file_path: file_path.to_path_buf(),
        remotes: frontmatter.get("remotes").and_then(json_to_string_vec),
        old_remotes: frontmatter.get("old_remotes").and_then(json_to_string_vec),
        slug: frontmatter
            .get("slug")
            .and_then(|v| v.as_str().map(|s| s.to_string())),
        old_slugs: frontmatter.get("old_slugs").and_then(json_to_string_vec),
    })
}

/// Get all project notes from the vault's projects/ folder.
///
/// Scans all notes in the graph index, filtering for those in the projects/ folder,
/// and loads their frontmatter to extract project metadata.
fn get_all_projects(graph_index: &GraphIndex, vault_path: &Path) -> Vec<ProjectMetadata> {
    let mut projects = Vec::new();

    for rel_path in graph_index.all_paths() {
        // Only consider notes in projects/ folder
        if !rel_path.starts_with("projects/") {
            continue;
        }

        // Extract note name from path
        let note_name = std::path::Path::new(rel_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();

        let file_path = vault_path.join(rel_path);
        if let Some(metadata) = load_project_metadata(note_name, &file_path) {
            projects.push(metadata);
        }
    }

    projects
}

/// Match result from strict or loose matching
struct MatchResult {
    matched: bool,
    on: Option<MatchedOn>,
    value: Option<String>,
}

/// Check if a project matches a directory via strict matching (current remotes/slug).
fn is_strict_match(project: &ProjectMetadata, directory: &DirectoryInfo) -> MatchResult {
    // Check current remotes
    if let Some(ref project_remotes) = project.remotes
        && !directory.git_remotes.is_empty()
    {
        let normalized_project_remotes: Vec<String> = project_remotes
            .iter()
            .map(|r| normalize_remote(r))
            .collect();
        let normalized_dir_remotes: Vec<String> = directory
            .git_remotes
            .iter()
            .map(|r| normalize_remote(r))
            .collect();

        for proj_remote in &normalized_project_remotes {
            for dir_remote in &normalized_dir_remotes {
                if proj_remote == dir_remote {
                    return MatchResult {
                        matched: true,
                        on: Some(MatchedOn::Remote),
                        value: Some(dir_remote.clone()),
                    };
                }
            }
        }
    }

    // Check current slug (case-insensitive exact match)
    if let Some(ref slug) = project.slug
        && slug.to_lowercase() == directory.name.to_lowercase()
    {
        return MatchResult {
            matched: true,
            on: Some(MatchedOn::Slug),
            value: Some(directory.name.clone()),
        };
    }

    MatchResult {
        matched: false,
        on: None,
        value: None,
    }
}

/// Check if a project matches a directory via loose matching (old remotes/slugs).
/// This indicates a disconnect - the project was previously linked but remote/dir was renamed.
fn is_loose_match(project: &ProjectMetadata, directory: &DirectoryInfo) -> MatchResult {
    // Check old remotes
    if let Some(ref old_remotes) = project.old_remotes
        && !directory.git_remotes.is_empty()
    {
        let normalized_old_remotes: Vec<String> =
            old_remotes.iter().map(|r| normalize_remote(r)).collect();
        let normalized_dir_remotes: Vec<String> = directory
            .git_remotes
            .iter()
            .map(|r| normalize_remote(r))
            .collect();

        for old_remote in &normalized_old_remotes {
            for dir_remote in &normalized_dir_remotes {
                if old_remote == dir_remote {
                    return MatchResult {
                        matched: true,
                        on: Some(MatchedOn::OldRemote),
                        value: Some(dir_remote.clone()),
                    };
                }
            }
        }
    }

    // Check old slugs (case-insensitive exact match)
    if let Some(ref old_slugs) = project.old_slugs {
        for old_slug in old_slugs {
            if old_slug.to_lowercase() == directory.name.to_lowercase() {
                return MatchResult {
                    matched: true,
                    on: Some(MatchedOn::OldSlug),
                    value: Some(directory.name.clone()),
                };
            }
        }
    }

    MatchResult {
        matched: false,
        on: None,
        value: None,
    }
}

/// Find similar project names for suggestions when no match is found.
/// Uses simple case-insensitive substring matching.
fn find_similar_projects(dir_name: &str, all_projects: &[ProjectMetadata]) -> Vec<ProjectMetadata> {
    let lower_dir_name = dir_name.to_lowercase();
    let mut similar = Vec::new();

    for project in all_projects {
        let lower_project_name = project.name.to_lowercase();
        let lower_slug = project.slug.as_ref().map(|s| s.to_lowercase());

        // Check if directory name is substring of project name or slug
        let matches = lower_project_name.contains(&lower_dir_name)
            || lower_slug
                .as_ref()
                .map(|s| s.contains(&lower_dir_name))
                .unwrap_or(false)
            || lower_dir_name.contains(&lower_project_name)
            || lower_slug
                .as_ref()
                .map(|s| lower_dir_name.contains(s.as_str()))
                .unwrap_or(false);

        if matches {
            similar.push(project.clone());
        }
    }

    similar
}

/// Discover projects for a working directory.
///
/// Algorithm:
/// 1. Crawl from CWD up to home directory
/// 2. For each directory, extract git remotes and directory name
/// 3. Search all project notes for strict matches (current remotes/slug)
/// 4. If no strict match, search for loose matches (old remotes/slugs)
/// 5. If no matches at all, find similar projects for suggestions
/// 6. Return all matches ordered by depth (closest first)
pub fn discover_projects(
    cwd: &Path,
    graph_index: &GraphIndex,
    vault_path: &Path,
) -> DiscoveryResult {
    let directories = crawl_directories(cwd);
    let all_projects = get_all_projects(graph_index, vault_path);

    let mut strict_matches = Vec::new();
    let mut loose_matches = Vec::new();
    let mut all_git_remotes = HashSet::new();

    // Collect all git remotes from all directories
    for dir in &directories {
        for remote in &dir.git_remotes {
            all_git_remotes.insert(remote.clone());
        }
    }

    // Check each directory against all projects
    for directory in &directories {
        for project in &all_projects {
            // Try strict match first
            let strict = is_strict_match(project, directory);
            if strict.matched {
                strict_matches.push(DiscoveredProject {
                    metadata: project.clone(),
                    match_type: MatchType::Strict,
                    matched_on: strict.on,
                    matched_value: strict.value,
                    depth: directory.depth,
                });
                continue;
            }

            // Try loose match if no strict match
            let loose = is_loose_match(project, directory);
            if loose.matched {
                loose_matches.push(DiscoveredProject {
                    metadata: project.clone(),
                    match_type: MatchType::Loose,
                    matched_on: loose.on,
                    matched_value: loose.value,
                    depth: directory.depth,
                });
            }
        }
    }

    // Find suggestions if no matches
    let suggestions = if strict_matches.is_empty() && loose_matches.is_empty() {
        let cwd_name = directories
            .first()
            .map(|d| d.name.as_str())
            .unwrap_or_default();
        find_similar_projects(cwd_name, &all_projects)
    } else {
        Vec::new()
    };

    DiscoveryResult {
        cwd: cwd.to_path_buf(),
        git_remotes: all_git_remotes.into_iter().collect(),
        searched_paths: directories.iter().map(|d| d.path.clone()).collect(),
        strict_matches,
        loose_matches,
        suggestions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_remote_https() {
        assert_eq!(
            normalize_remote("https://github.com/user/repo.git"),
            "github.com/user/repo"
        );
        assert_eq!(
            normalize_remote("https://github.com/user/repo"),
            "github.com/user/repo"
        );
    }

    #[test]
    fn test_normalize_remote_ssh() {
        assert_eq!(
            normalize_remote("git@github.com:user/repo.git"),
            "github.com/user/repo"
        );
        assert_eq!(
            normalize_remote("git@bitbucket.org:user/repo"),
            "bitbucket.org/user/repo"
        );
    }

    #[test]
    fn test_normalize_remote_trailing_slash() {
        assert_eq!(
            normalize_remote("https://github.com/user/repo/"),
            "github.com/user/repo"
        );
    }

    #[test]
    fn test_normalize_remote_case_insensitive() {
        assert_eq!(
            normalize_remote("https://GitHub.com/User/Repo.git"),
            "github.com/user/repo"
        );
    }

    #[test]
    fn test_strict_match_by_remote() {
        let project = ProjectMetadata {
            name: "Test Project".to_string(),
            file_path: PathBuf::from("/vault/projects/Test Project.md"),
            remotes: Some(vec!["git@github.com:user/test.git".to_string()]),
            old_remotes: None,
            slug: None,
            old_slugs: None,
        };

        let directory = DirectoryInfo {
            path: PathBuf::from("/code/test"),
            name: "test".to_string(),
            git_remotes: vec!["https://github.com/user/test.git".to_string()],
            depth: 0,
        };

        let result = is_strict_match(&project, &directory);
        assert!(result.matched);
        assert_eq!(result.on, Some(MatchedOn::Remote));
    }

    #[test]
    fn test_strict_match_by_slug() {
        let project = ProjectMetadata {
            name: "Test Project".to_string(),
            file_path: PathBuf::from("/vault/projects/Test Project.md"),
            remotes: None,
            old_remotes: None,
            slug: Some("test-project".to_string()),
            old_slugs: None,
        };

        let directory = DirectoryInfo {
            path: PathBuf::from("/code/test-project"),
            name: "test-project".to_string(),
            git_remotes: vec![],
            depth: 0,
        };

        let result = is_strict_match(&project, &directory);
        assert!(result.matched);
        assert_eq!(result.on, Some(MatchedOn::Slug));
    }

    #[test]
    fn test_loose_match_by_old_remote() {
        let project = ProjectMetadata {
            name: "Test Project".to_string(),
            file_path: PathBuf::from("/vault/projects/Test Project.md"),
            remotes: Some(vec!["git@github.com:newuser/test.git".to_string()]),
            old_remotes: Some(vec!["git@github.com:olduser/test.git".to_string()]),
            slug: None,
            old_slugs: None,
        };

        let directory = DirectoryInfo {
            path: PathBuf::from("/code/test"),
            name: "test".to_string(),
            git_remotes: vec!["https://github.com/olduser/test.git".to_string()],
            depth: 0,
        };

        let result = is_loose_match(&project, &directory);
        assert!(result.matched);
        assert_eq!(result.on, Some(MatchedOn::OldRemote));
    }

    #[test]
    fn test_loose_match_by_old_slug() {
        let project = ProjectMetadata {
            name: "Test Project".to_string(),
            file_path: PathBuf::from("/vault/projects/Test Project.md"),
            remotes: None,
            old_remotes: None,
            slug: Some("new-name".to_string()),
            old_slugs: Some(vec!["old-name".to_string()]),
        };

        let directory = DirectoryInfo {
            path: PathBuf::from("/code/old-name"),
            name: "old-name".to_string(),
            git_remotes: vec![],
            depth: 0,
        };

        let result = is_loose_match(&project, &directory);
        assert!(result.matched);
        assert_eq!(result.on, Some(MatchedOn::OldSlug));
    }

    #[test]
    fn test_find_similar_projects() {
        let projects = vec![
            ProjectMetadata {
                name: "obsidian-memory".to_string(),
                file_path: PathBuf::from("/vault/projects/obsidian-memory.md"),
                remotes: None,
                old_remotes: None,
                slug: None,
                old_slugs: None,
            },
            ProjectMetadata {
                name: "other-project".to_string(),
                file_path: PathBuf::from("/vault/projects/other-project.md"),
                remotes: None,
                old_remotes: None,
                slug: None,
                old_slugs: None,
            },
        ];

        let similar = find_similar_projects("obsidian", &projects);
        assert_eq!(similar.len(), 1);
        assert_eq!(similar[0].name, "obsidian-memory");
    }
}
