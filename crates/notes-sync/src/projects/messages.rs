//! Message generation for project discovery results

use std::path::Path;

use super::types::{DiscoveryResult, MatchedOn};

/// Generate status message for project discovery results.
/// Returns appropriate message based on what was found.
pub fn generate_discovery_status_message(discovery_result: &DiscoveryResult, cwd: &Path) -> String {
    // Strict matches - just return list of loaded projects
    if !discovery_result.strict_matches.is_empty() {
        let project_names: Vec<String> = discovery_result
            .strict_matches
            .iter()
            .map(|m| format!("[[{}]]", m.metadata.name))
            .collect();
        return format!("Projects auto-loaded: {}", project_names.join(", "));
    }

    // Disconnect detected (loose match)
    if !discovery_result.loose_matches.is_empty() {
        let m = &discovery_result.loose_matches[0];
        let matched_on_str = m.matched_on.map(|o| o.as_str()).unwrap_or("unknown");

        let mut message = format!(
            "**Project disconnect detected**\n\nFound [[{}]] via {} match.\n\n",
            m.metadata.name, matched_on_str
        );

        match m.matched_on {
            Some(MatchedOn::OldRemote) => {
                let current_remote = discovery_result
                    .git_remotes
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");
                let expected = m
                    .metadata
                    .remotes
                    .as_ref()
                    .map(|r| r.join(", "))
                    .unwrap_or_else(|| "none".to_string());

                message.push_str(&format!(
                    "Current: {}\nExpected: {}\n\nUpdate frontmatter to move old remote to old_remotes array.",
                    current_remote, expected
                ));
            }
            Some(MatchedOn::OldSlug) => {
                let cwd_name = cwd
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let expected = m.metadata.slug.as_deref().unwrap_or("none");

                message.push_str(&format!(
                    "Current: {}\nExpected: {}\n\nUpdate frontmatter to move old slug to old_slugs array.",
                    cwd_name, expected
                ));
            }
            _ => {}
        }

        return message;
    }

    // No match found - with suggestions
    let cwd_name = cwd
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let remotes_str = if discovery_result.git_remotes.is_empty() {
        "none".to_string()
    } else {
        discovery_result.git_remotes.join(", ")
    };

    if !discovery_result.suggestions.is_empty() {
        let suggestions: Vec<String> = discovery_result
            .suggestions
            .iter()
            .map(|p| format!("- [[{}]]", p.name))
            .collect();

        return format!(
            "**No project found**\n\nDir: {} | Remotes: {}\n\nSimilar:\n{}\n\nExisting project or new?",
            cwd_name,
            remotes_str,
            suggestions.join("\n")
        );
    }

    // No match and no suggestions
    format!(
        "**No project found**\n\nDir: {} | Remotes: {}\n\nCreate project note with appropriate frontmatter.",
        cwd_name, remotes_str
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::{DiscoveredProject, MatchType, ProjectMetadata};
    use std::path::PathBuf;

    #[test]
    fn test_strict_matches_message() {
        let result = DiscoveryResult {
            cwd: PathBuf::from("/code/test"),
            git_remotes: vec!["git@github.com:user/test.git".to_string()],
            searched_paths: vec![PathBuf::from("/code/test")],
            strict_matches: vec![DiscoveredProject {
                metadata: ProjectMetadata {
                    name: "Test Project".to_string(),
                    file_path: PathBuf::from("/vault/projects/Test Project.md"),
                    remotes: Some(vec!["git@github.com:user/test.git".to_string()]),
                    old_remotes: None,
                    slug: None,
                    old_slugs: None,
                },
                match_type: MatchType::Strict,
                matched_on: Some(MatchedOn::Remote),
                matched_value: Some("github.com/user/test".to_string()),
                depth: 0,
            }],
            loose_matches: vec![],
            suggestions: vec![],
        };

        let message = generate_discovery_status_message(&result, &PathBuf::from("/code/test"));
        assert_eq!(message, "Projects auto-loaded: [[Test Project]]");
    }

    #[test]
    fn test_multiple_strict_matches_message() {
        let result = DiscoveryResult {
            cwd: PathBuf::from("/code/company/project"),
            git_remotes: vec!["git@github.com:company/project.git".to_string()],
            searched_paths: vec![
                PathBuf::from("/code/company/project"),
                PathBuf::from("/code/company"),
            ],
            strict_matches: vec![
                DiscoveredProject {
                    metadata: ProjectMetadata {
                        name: "My Project".to_string(),
                        file_path: PathBuf::from("/vault/projects/My Project.md"),
                        remotes: Some(vec!["git@github.com:company/project.git".to_string()]),
                        old_remotes: None,
                        slug: None,
                        old_slugs: None,
                    },
                    match_type: MatchType::Strict,
                    matched_on: Some(MatchedOn::Remote),
                    matched_value: Some("github.com/company/project".to_string()),
                    depth: 0,
                },
                DiscoveredProject {
                    metadata: ProjectMetadata {
                        name: "Company".to_string(),
                        file_path: PathBuf::from("/vault/projects/Company.md"),
                        remotes: None,
                        old_remotes: None,
                        slug: Some("company".to_string()),
                        old_slugs: None,
                    },
                    match_type: MatchType::Strict,
                    matched_on: Some(MatchedOn::Slug),
                    matched_value: Some("company".to_string()),
                    depth: 1,
                },
            ],
            loose_matches: vec![],
            suggestions: vec![],
        };

        let message =
            generate_discovery_status_message(&result, &PathBuf::from("/code/company/project"));
        assert_eq!(message, "Projects auto-loaded: [[My Project]], [[Company]]");
    }

    #[test]
    fn test_no_match_no_suggestions() {
        let result = DiscoveryResult {
            cwd: PathBuf::from("/code/unknown"),
            git_remotes: vec!["git@github.com:user/unknown.git".to_string()],
            searched_paths: vec![PathBuf::from("/code/unknown")],
            strict_matches: vec![],
            loose_matches: vec![],
            suggestions: vec![],
        };

        let message = generate_discovery_status_message(&result, &PathBuf::from("/code/unknown"));
        assert!(message.contains("**No project found**"));
        assert!(message.contains("Dir: unknown"));
        assert!(message.contains("git@github.com:user/unknown.git"));
        assert!(message.contains("Create project note"));
    }

    #[test]
    fn test_no_match_with_suggestions() {
        let result = DiscoveryResult {
            cwd: PathBuf::from("/code/obsidian"),
            git_remotes: vec![],
            searched_paths: vec![PathBuf::from("/code/obsidian")],
            strict_matches: vec![],
            loose_matches: vec![],
            suggestions: vec![ProjectMetadata {
                name: "obsidian-memory".to_string(),
                file_path: PathBuf::from("/vault/projects/obsidian-memory.md"),
                remotes: None,
                old_remotes: None,
                slug: None,
                old_slugs: None,
            }],
        };

        let message = generate_discovery_status_message(&result, &PathBuf::from("/code/obsidian"));
        assert!(message.contains("**No project found**"));
        assert!(message.contains("Similar:"));
        assert!(message.contains("[[obsidian-memory]]"));
        assert!(message.contains("Existing project or new?"));
    }
}
